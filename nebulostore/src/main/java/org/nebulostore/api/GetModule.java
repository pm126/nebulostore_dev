package org.nebulostore.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.crypto.SecretKey;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.message.InitSessionEndMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
import org.nebulostore.crypto.session.message.LocalInitSessionMessage;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.messages.GetObjectsMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectsMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Job module that fetches full object of type V or its fragment from NebuloStore.
 * Dependencies: object address.
 *
 * @param <V> Returning type.
 * @author Bolek Kulbabinski
 * @author Piotr Malicki
 */
public abstract class GetModule<V> extends ReturningJobModule<V> {
  private static Logger logger_ = Logger.getLogger(GetModule.class);

  private static final long REPLICA_WAIT_MILLIS = 5000L;
  protected EncryptionAPI encryption_;
  protected CommAddress myAddress_;
  protected Timer timer_;
  protected String privateKeyPeerId_;
  protected final Map<ObjectId, List<String>> currentVersionsMap_ = new HashMap<>();
  protected final Set<ObjectId> objectIds_ = new HashSet<>();
  protected final Map<String, String> timerTaskIds_ = new HashMap<>();
  protected Provider<ObjectRecreator> recreatorProvider_;
  protected final Map<ObjectId, ObjectRecreator> recreators_ = new HashMap<>();
  protected ReplicationGroup replicationGroup_;
  protected final Map<ObjectId, EncryptedObject> downloadedObjects_ = new HashMap<>();

  protected Set<NebuloAddress> addresses_;
  protected Map<String, SecretKey> sessionKeys_ = new HashMap<String, SecretKey>();
  protected GetModuleMode mode_;

  @Inject
  public void setDependencies(CommAddress myAddress,
      Timer timer, EncryptionAPI encryptionAPI,
      @Named("PrivateKeyPeerId") String privateKeyPeerId,
      Provider<ObjectRecreator> recreatorProvider) {
    myAddress_ = myAddress;
    timer_ = timer;
    encryption_ = encryptionAPI;
    privateKeyPeerId_ = privateKeyPeerId;
    recreatorProvider_ = recreatorProvider;
  }

  public void fetchObject(NebuloAddress address) {
    fetchObject(Sets.newHashSet(address), GetModuleMode.STRICT);
  }

  protected void fetchObject(Set<NebuloAddress> addresses, GetModuleMode mode) {
    logger_.debug("Fetching objects: " + addresses + " by module: " + this + " with jobId: " +
        jobId_);
    addresses_ = addresses;
    mode_ = mode;
    for (NebuloAddress address : addresses) {
      objectIds_.add(address.getObjectId());
    }
    runThroughDispatcher();
  }

  public enum GetModuleMode {
    FLEXIBLE, STRICT
  }

  /**
   * States of the state machine.
   */
  protected enum STATE {
    INIT, REPLICA_FETCH, FILES_RECEIVED
  };

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected abstract class GetModuleVisitor extends MessageVisitor {

    protected final Map<ObjectId, Set<CommAddress>> replicatorsSets_ = new HashMap<>();
    protected final Map<CommAddress, Map<String, Set<ObjectId>>> waitingForRequests_ =
        new HashMap<>();
    protected STATE state_ = STATE.INIT;
    private boolean isProcessingDHTQuery_;

    public void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.info("Retrieving files " + addresses_);
      AppKey appKey = addresses_.iterator().next().getAppKey();

      if (state_ == STATE.INIT) {
        // State 1 - Send groupId to DHT and wait for reply.
        isProcessingDHTQuery_ = true;
        logger_.debug("Adding GetDHT to network queue (" + appKey + ", " + jobId_ + ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(appKey.getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ValueDHTMessage message) {
      if (state_ == STATE.INIT && isProcessingDHTQuery_) {
        // State 2 - Receive reply from DHT and iterate over logical path segments asking
        // for consecutive parts.
        isProcessingDHTQuery_ = false;

        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        Metadata metadata = (Metadata) message.getValue().getValue();
        logger_.debug("Received ValueDHTMessage: " + metadata.toString());
        ContractList contractList = metadata.getContractList();
        for (ObjectId objectId : objectIds_) {
          if (replicationGroup_ == null) {
            replicationGroup_ = contractList.getGroup(objectId);
          } else if (!replicationGroup_.equals(contractList.getGroup(objectId))) {
            timer_.cancelTimer();
            endWithError(new NebuloException("Objects to download are not in the same " +
                "replication group"));
            return;
          }
        }
        state_ = STATE.REPLICA_FETCH;
        if (replicationGroup_ == null) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          try {
            initRecreators();
          } catch (NebuloException e) {
            endWithError(e);
            return;
          }

          recalculateReplicatorsToAsk(recreators_.keySet());
          queryNextReplicas();
        }
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ErrorDHTMessage message) {
      if (state_ == STATE.INIT && isProcessingDHTQuery_) {
        logger_.debug("Received ErrorDHTMessage");
        timer_.cancelTimer();
        endWithError(new NebuloException("Could not fetch metadata from DHT.",
            message.getException()));
      } else {
        incorrectState(state_.name(), message);
      }
    }


    public abstract void visit(SendObjectsMessage message);

    public void visit(TimeoutMessage message) {
      if (state_ == STATE.REPLICA_FETCH && message.getMessageContent() instanceof TimeoutData &&
          state_.equals(((TimeoutData) message.getMessageContent()).state_)) {
        TimeoutData timeoutData = (TimeoutData) message.getMessageContent();
        logger_.debug("Timeout - request with id " + timeoutData.requestId_ + " sent to peer " +
            timeoutData.replicator_ + " failed. Recalculating peers to ask.");
        failRequest(timeoutData.replicator_, timeoutData.requestId_,
            message.getClass().getSimpleName());
      }
    }

    public void visit(InitSessionEndMessage message) {
      logger_.debug("Process " + message);
      Map<String, Set<ObjectId>> requestsMap = waitingForRequests_.get(message.getPeerAddress());
      if (requestsMap != null && requestsMap.containsKey(message.getData())) {
        sessionKeys_.put(message.getSessionId(), message.getSessionKey());
        networkQueue_.add(new GetObjectsMessage(CryptoUtils.getRandomId().toString(), myAddress_,
            message.getPeerAddress(), requestsMap.get(message.getData()),
            jobId_, message.getSessionId(), (String) message.getData()));
      } else {
        logger_.warn("Received " + message.getClass().getSimpleName() +
            " for an unknown request id");
      }
    }

    public void visit(InitSessionEndWithErrorMessage message) {
      logger_.debug("Process InitSessionEndWithErrorMessage " + message);
      failRequest(message.getPeerAddress(), (String) message.getData(),
          message.getClass().getSimpleName());
    }

    public void visit(ReplicatorErrorMessage message) {
      logger_.warn("ReplicatorErrorMessage received from peer " + message.getSourceAddress() +
          " with message: " + message.getMessage());
      if (state_ == STATE.REPLICA_FETCH) {
        failRequest(message.getSourceAddress(), message.getRequestId(),
            message.getClass().getSimpleName());
      } else {
        incorrectState(state_.name(), message);
      }
    }

    protected boolean tryRecreateEncryptedObjects(SendObjectsMessage message)
        throws NebuloException {
      logger_.debug("Received " + message);
      Set<ObjectId> objectIds = waitingForRequests_.get(message.getSourceAddress()).
          remove(message.getRequestId());
      if (objectIds != null) {
        if (state_ == STATE.REPLICA_FETCH) {
          timer_.cancelTask(timerTaskIds_.get(message.getRequestId()));
          boolean replicatorsToAskChanged = false;

          for (ObjectId objectId : objectIds) {
            boolean versionUpdated = false;
            replicatorsSets_.get(objectId).remove(message.getSourceAddress());
            try {
              if (checkVersion(objectId, message.getVersionsMap().get(objectId))) {
                // Versions were updated, starting again
                recreators_.get(objectId).clearReceivedFragments();
                versionUpdated = true;
              }
            } catch (NebuloException e) {
              logger_.warn(e);
              recreators_.get(objectId).removeReplicator(message.getSourceAddress());
              recalculateReplicatorsToAsk(Sets.newHashSet(objectId));
              replicatorsToAskChanged = true;
              continue;
            }

            logger_.debug("Got next file fragment for objectId: " + objectId);

            if (recreators_.get(objectId).addNextFragment(decryptWithSessionKey(
                message.getEncryptedEntities().get(objectId),
                message.getSessionId()), message.getSourceAddress())) {
              logger_.debug("Got object with id: " + objectId);
              downloadedObjects_.put(objectId, recreators_.get(objectId).recreateObject(
                  message.getObjectsSizes().get(objectId)));
            } else if (versionUpdated) {
              recalculateReplicatorsToAsk(Sets.newHashSet(objectId));
              replicatorsToAskChanged = true;
            }
          }

          if (downloadedObjects_.keySet().equals(objectIds_)) {
            logger_.debug("Got objects - returning");

            // State 3 - Finally got the files, return it;
            state_ = STATE.FILES_RECEIVED;
            return true;
          }

          if (replicatorsToAskChanged) {
            queryNextReplicas();
          }

          return false;

        } else {
          logger_.warn("SendObjectMessage received in state " + state_);
          return false;
        }
      } else {
        logger_.warn("Received an object fragment from an unexpected replicator.");
        return false;
      }
    }

    protected void initRecreators() throws NebuloException {
      for (ObjectId objectId : objectIds_) {
        ObjectRecreator recreator = recreatorProvider_.get();
        recreator.initRecreator(replicationGroup_.getReplicators(), null);
        recreators_.put(objectId, recreator);
      }
    }

    private void failRequest(CommAddress replicator, String requestId, String messageType) {
      Map<String, Set<ObjectId>> requestsMap = waitingForRequests_.get(replicator);
      if (requestsMap != null && requestsMap.containsKey(requestId)) {
        Set<ObjectId> objectIds = requestsMap.remove(requestId);
        timer_.cancelTask(timerTaskIds_.remove(requestId));
        for (ObjectId objectId : objectIds) {
          recreators_.get(objectId).removeReplicator(replicator);
        }
        recalculateReplicatorsToAsk(objectIds);
        queryNextReplicas();
      } else {
        logger_.warn("Received " + messageType + " for an unknown request id");
      }
    }

    private void recalculateReplicatorsToAsk(Set<ObjectId> objectIds) {
      Map<ObjectId, Set<CommAddress>> waitingForReplicatorsMap = new HashMap<>();
      for (ObjectId objectId : objectIds) {
        waitingForReplicatorsMap.put(objectId, new HashSet<CommAddress>());
      }
      for (Entry<CommAddress, Map<String, Set<ObjectId>>> requestsEntry :
          waitingForRequests_.entrySet()) {
        CommAddress replicator = requestsEntry.getKey();
        Map<String, Set<ObjectId>> requestMap = requestsEntry.getValue();
        Set<ObjectId> objectsInRequests = new HashSet<>();
        for (Set<ObjectId> requestObjects : requestMap.values()) {
          objectsInRequests.addAll(Sets.intersection(objectIds, requestObjects));
        }

        for (ObjectId objectId : objectsInRequests) {
          waitingForReplicatorsMap.get(objectId).add(replicator);
        }
      }

      for (Entry<ObjectId, Set<CommAddress>> entry : waitingForReplicatorsMap.entrySet()) {
        ObjectId objectId = entry.getKey();
        replicatorsSets_.put(objectId, Sets.newHashSet(recreators_.get(objectId).
            calcReplicatorsToAsk(entry.getValue())));
      }
    }

    protected abstract void endWithResult();

    protected void queryNextReplicas() {
      logger_.debug("Querying next replicas. Replicators to ask: " + replicatorsSets_ +
          ", waiting for requests" + waitingForRequests_ + ", timer tasks: " + timerTaskIds_);

      boolean anyObjectFailed = false;
      boolean allObjectsEnded = true;
      boolean allObjectsFailed = true;
      for (Entry<ObjectId, Set<CommAddress>> entry : replicatorsSets_.entrySet()) {
        ObjectId objectId = entry.getKey();
        Set<CommAddress> replicatorsSet = entry.getValue();
        boolean objectFailed = replicatorsSet.isEmpty() &&
            !downloadedObjects_.keySet().contains(objectId);
        anyObjectFailed = anyObjectFailed || objectFailed;
        allObjectsEnded = allObjectsEnded && replicatorsSet.isEmpty();
        allObjectsFailed = allObjectsFailed && objectFailed;
      }

      if (mode_.equals(GetModuleMode.STRICT) && anyObjectFailed) {
        logger_.info("Querying next replicas. Replicators to ask: " + replicatorsSets_ +
            ", waiting for requests" + waitingForRequests_ + ", timer tasks: " + timerTaskIds_);
        timer_.cancelTimer();
        endWithError(new NebuloException("Not enough replicas responded in time."));
      } else if (mode_.equals(GetModuleMode.FLEXIBLE) && allObjectsEnded) {
        timer_.cancelTimer();
        if (allObjectsFailed) {
          endWithError(new NebuloException("Not enough replicas responded in time."));
        } else {
          endWithResult();
        }
      } else {
        Map<CommAddress, Set<ObjectId>> replicatorsObjectsMap = new HashMap<>();
        for (Entry<ObjectId, Set<CommAddress>> entry : replicatorsSets_.entrySet()) {
          for (CommAddress replicator : entry.getValue()) {
            if (!waitingForRequests_.containsKey(replicator) ||
                !isWaitingForObject(replicator, entry.getKey())) {
              if (!replicatorsObjectsMap.containsKey(replicator)) {
                replicatorsObjectsMap.put(replicator, new HashSet<ObjectId>());
              }
              replicatorsObjectsMap.get(replicator).add(entry.getKey());
            }
          }
        }
        logger_.debug("Replicators objects: " + replicatorsObjectsMap);
        Iterator<CommAddress> iterator = replicatorsObjectsMap.keySet().iterator();
        while (iterator.hasNext()) {
          CommAddress replicator = iterator.next();
          logger_.debug("Querying replica (" + replicator + ")");
          if (!waitingForRequests_.containsKey(replicator)) {
            waitingForRequests_.put(replicator, new HashMap<String, Set<ObjectId>>());
          }
          String requestId = CryptoUtils.getRandomString();
          waitingForRequests_.get(replicator).put(requestId, replicatorsObjectsMap.get(replicator));
          startSessionAgreement(replicator, requestId);
          timerTaskIds_.put(requestId, timer_.schedule(jobId_, REPLICA_WAIT_MILLIS,
              new TimeoutData(STATE.REPLICA_FETCH, replicator, requestId), false));
        }
      }
    }

    private boolean isWaitingForObject(CommAddress replicator, ObjectId objectId) {
      for (Set<ObjectId> requestedObjects : waitingForRequests_.get(replicator).values()) {
        if (requestedObjects.contains(objectId)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Check object version received in SendObjectMessage and update it if necessary.
     *
     * @param message
     * @return true if the version was changed to newer, false otherwise
     * @throws NebuloException
     *           when received fragment is not usable because of incorrect version
     */
    protected boolean checkVersion(ObjectId objectId, List<String> remoteVersions)
        throws NebuloException {
      logger_.debug("Current versions: " + currentVersionsMap_.get(objectId) +
          "\n Remote versions: " + remoteVersions);
      if (!currentVersionsMap_.containsKey(objectId)) {
        currentVersionsMap_.put(objectId, remoteVersions);
        return false;
      }

      List<String> currentVersions = currentVersionsMap_.get(objectId);
      if (remoteVersions.size() >= currentVersions.size() &&
          remoteVersions.subList(0, currentVersions.size()).equals(currentVersions)) {
        if (remoteVersions.size() > currentVersions.size()) {
          currentVersionsMap_.put(objectId, remoteVersions);
          return true;
        }
      } else {
        throw new NebuloException("Received an outdated object fragment.");
      }

      return false;
    }

    protected void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }

    private void startSessionAgreement(CommAddress replicator, Serializable data) {
      outQueue_.add(new LocalInitSessionMessage(replicator, getJobId(), data));
    }

    protected EncryptedObject decryptWithSessionKey(EncryptedObject cipher, String sessionId)
        throws CryptoException {
      return (EncryptedObject) encryption_.decryptWithSessionKey(cipher,
          sessionKeys_.remove(sessionId));
    }
  }

  protected static class TimeoutData implements Serializable {
    private static final long serialVersionUID = -7227307736422504225L;

    public STATE state_;
    public CommAddress replicator_;
    public String requestId_;

    public TimeoutData(STATE state, CommAddress replicator, String requestId) {
      state_ = state;
      replicator_ = replicator;
      requestId_ = requestId;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " with jobId_=" + jobId_;
  }
}
