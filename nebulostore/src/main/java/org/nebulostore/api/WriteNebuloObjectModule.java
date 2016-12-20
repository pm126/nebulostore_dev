package org.nebulostore.api;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectWriter;
import org.nebulostore.async.SendAsynchronousMessagesForPeerModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.UpdateNebuloObjectMessage;
import org.nebulostore.broker.messages.RegisterObjectMessage;
import org.nebulostore.broker.messages.WriteFinishedMessage;
import org.nebulostore.broker.messages.WritePermissionRejectionMessage;
import org.nebulostore.broker.messages.WritePermissionRequestMessage;
import org.nebulostore.broker.messages.WritePermissionResponseMessage;
import org.nebulostore.coding.ObjectRecreationChecker;
import org.nebulostore.coding.ReplicaPlacementData;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ObjectOutdatedMessage;
import org.nebulostore.timer.Timer;

/**
 * @author Bolek Kulbabinski
 * @author szymonmatejczyk
 */

public class WriteNebuloObjectModule extends WriteModule implements ObjectWriter {
  private static Logger logger_ = Logger.getLogger(WriteNebuloObjectModule.class);
  /* small files below 1MB */
  private static final int SMALL_FILE_THRESHOLD = 2 * 1024 * 1024;

  private NebuloObject object_;

  private List<String> previousVersionSHAs_;

  private final String publicKeyPeerId_;
  private final ReplicaPlacementPreparator replicaPlacementPreparator_;
  private final ObjectRecreationChecker recreationChecker_;
  private ReplicationGroup group_;

  @Inject
  public WriteNebuloObjectModule(EncryptionAPI encryption,
      @Named("PublicKeyPeerId") String publicKeyPeerId,
      ReplicaPlacementPreparator replicaPlacementPreparator, Timer timer,
      ObjectRecreationChecker recreationChecker) {
    super(encryption, timer);
    publicKeyPeerId_ = publicKeyPeerId;
    replicaPlacementPreparator_ = replicaPlacementPreparator;
    recreationChecker_ = recreationChecker;
  }

  @Override
  protected WriteModuleVisitor createVisitor() {
    return new StateMachineVisitor();
  }

  @Override
  public void writeObject(NebuloObject objectToWrite, List<String> previousVersionSHAs) {
    object_ = objectToWrite;
    logger_.debug("Object address: " + object_.getAddress());
    try {
      logger_.debug("Serialized object to write size: " +
          CryptoUtils.serializeObject(object_).length);
    } catch (CryptoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    previousVersionSHAs_ = previousVersionSHAs;
    super.writeObject();
  }

  @Override
  protected boolean gotRequiredConfirmations(Set<CommAddress> confirmedReplicators) {
    return recreationChecker_.isRecreationPossible(group_.getReplicators(), confirmedReplicators);
  }

  @Override
  protected void endModuleWithError(NebuloException exception) {
    outQueue_.add(new WriteFinishedMessage(jobId_, object_.getObjectId()));
    super.endModuleWithError(exception);
  }

  @Override
  protected void endModuleWithSuccess() {
    outQueue_.add(new WriteFinishedMessage(jobId_, object_.getObjectId()));
    super.endModuleWithSuccess();
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected class StateMachineVisitor extends WriteModuleVisitor {

    private boolean isProcessingDHTQuery_;
    private String commitVersion_;

    public void visit(JobInitMessage message) {
      if (state_ == STATE.INIT) {
        logger_.debug("Initializing...");
        // State 1 - Send groupId to DHT and wait for reply.
        isProcessingDHTQuery_ = true;
        jobId_ = message.getId();

        logger_.info("Asking broker for permission to write object");
        outQueue_.add(new WritePermissionRequestMessage(jobId_,
            object_.getAddress().getObjectId()));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(WritePermissionResponseMessage message) {
      if (state_ == STATE.INIT) {
        logger_.info("Received write permission from broker.");
        NebuloAddress address = object_.getAddress();
        logger_.debug("Adding GetDHT to network queue (" + address.getAppKey() + ", " +
            jobId_ + ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address.getAppKey().getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(WritePermissionRejectionMessage message) {
      if (state_ == STATE.INIT) {
        logger_.info("Received write permission rejection from broker");
        endWithError(new NebuloException("Could not write the object"));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ValueDHTMessage message) {
      logger_.debug("Got ValueDHTMessage " + message.toString());
      if (state_ == STATE.INIT && isProcessingDHTQuery_) {

        // Receive reply from DHT and iterate over logical path segments asking
        // for consecutive parts.
        isProcessingDHTQuery_ = false;

        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        // TODO(bolek): Merge this with similar part from GetNebuloFileModule?
        Metadata metadata = (Metadata) message.getValue().getValue();
        logger_.debug("Metadata: " + metadata);

        ContractList contractList = metadata.getContractList();
        logger_.debug("ContractList: " + contractList);
        group_ = contractList.getGroup(object_.getObjectId());

        logger_.debug("Group: " + group_);
        if (group_ == null) {
          endModuleWithError(new NebuloException("No peers replicating this object."));
        } else {
          try {
            EncryptedObject encryptedObject =
                encryption_.encrypt(object_, publicKeyPeerId_);
            commitVersion_ = CryptoUtils.sha(encryptedObject);
            boolean isSmallFile = encryptedObject.size() <= SMALL_FILE_THRESHOLD;

            logger_.debug("original size: " + encryptedObject.getEncryptedData().length);
            ReplicaPlacementData placementData =
                replicaPlacementPreparator_.prepareObject(encryptedObject.getEncryptedData(),
                group_.getReplicators());
            int sum = 0;
            for (Entry<CommAddress, EncryptedObject> entry : placementData.
                getReplicaPlacementMap().entrySet()) {
              sum += entry.getValue().size();
            }
            logger_.debug("Size: " + sum);
            ObjectId objectId = object_.getObjectId();
            sendStoreQueries(
                Collections.singletonMap(objectId, placementData.getReplicaPlacementMap()),
                Collections.singletonMap(objectId, encryptedObject.size()),
                Collections.singletonMap(objectId, previousVersionSHAs_),
                isSmallFile, Collections.singletonMap(objectId, commitVersion_));
          } catch (CryptoException exception) {
            endModuleWithError(new NebuloException("Unable to encrypt object.", exception));
          } catch (NebuloException exception) {
            endModuleWithError(new NebuloException("Unable to prepare replica placement data.",
                exception));
          }

        }
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ErrorDHTMessage message) {
      if (state_ == STATE.INIT && isProcessingDHTQuery_) {
        logger_.debug("Received ErrorDHTMessage");
        endModuleWithError(new NebuloException("Could not fetch metadata from DHT.",
            message.getException()));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    @Override
    protected void respondToAnswer(TransactionAnswerInMessage message) {
      super.respondToAnswer(message);
      if (message.answer_ == TransactionAnswer.COMMIT) {
        logger_.debug("Commiting, dead replicators: " + unavailableReplicators_);
        // Peers that didn't respond should get an AM.
        for (CommAddress deadReplicator : unavailableReplicators_) {
          AsynchronousMessage asynchronousMessage = new UpdateNebuloObjectMessage(
              object_.getAddress());
          new SendAsynchronousMessagesForPeerModule(deadReplicator, asynchronousMessage, outQueue_);
        }

        // Peers that rejected or withheld transaction should get notification, that their
        // version is outdated.
        for (CommAddress rejecting : rejectingOrWithholdingReplicators_) {
          networkQueue_.add(new ObjectOutdatedMessage(rejecting, object_.getAddress()));
        }

        // TODO(szm): don't like updating version here
        object_.newVersionCommitted(commitVersion_);
        // TODO(pm): Is it always necessary?
        outQueue_.add(new RegisterObjectMessage(object_.getObjectId()));
      } else {
        logger_.info("Aborting the transaction, dead replicators: " + unavailableReplicators_);
      }
    }
  }
}
