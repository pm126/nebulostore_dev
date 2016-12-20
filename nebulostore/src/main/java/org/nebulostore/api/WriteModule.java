package org.nebulostore.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.TwoStepReturningJobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.message.InitSessionEndMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
import org.nebulostore.crypto.session.message.LocalInitSessionMessage;
import org.nebulostore.replicator.core.StoreData;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectsMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;
import org.nebulostore.replicator.messages.UpdateRejectMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * @author Bolek Kulbabinski
 * @author szymonmatejczyk
 */

public abstract class WriteModule extends TwoStepReturningJobModule<Void, Void, TransactionAnswer> {
  private static Logger logger_ = Logger.getLogger(WriteNebuloObjectModule.class);

  private static final int REPLICA_RESPONSE_TIMEOUT_MILIS = 60000;

  private final WriteModuleVisitor visitor_;

  private final Map<CommAddress, Map<ObjectId, EncryptedObject>> objectsToSave_ = new HashMap<>();
  protected Map<ObjectId, Integer> objectSizesMap_;

  protected final EncryptionAPI encryption_;
  private final Timer timer_;

  /**
   * States of the state machine.
   */
  protected enum STATE {
    INIT, REPLICA_UPDATE, RETURNED_WAITING_FOR_REST, DONE
  };

  protected WriteModule(EncryptionAPI encryption, Timer timer) {
    encryption_ = encryption;
    visitor_ = createVisitor();
    timer_ = timer;
  }

  protected abstract WriteModuleVisitor createVisitor();

  protected abstract boolean gotRequiredConfirmations(Set<CommAddress> confirmedReplicators);

  protected void writeObject() {
    logger_.debug("Writing object, module: " + this);
    runThroughDispatcher();
  }

  protected void endModuleWithError(NebuloException exception) {
    endWithError(exception);
  }

  protected void endModuleWithSuccess() {
    endWithSuccess(null);
  }

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected abstract class WriteModuleVisitor extends MessageVisitor {
    protected STATE state_;
    /* Recipients we are waiting answer from. */
    protected final Set<CommAddress> recipientsSet_ = new HashSet<>();

    /* Repicators that rejected transaction, when it has been already commited. */
    protected final Set<CommAddress> rejectingOrWithholdingReplicators_ = new HashSet<>();

    /* CommAddress -> JobId of peers waiting for transaction result */
    protected final Map<CommAddress, String> waitingForTransactionResult_ = new HashMap<>();

    protected final Set<CommAddress> unavailableReplicators_ = new HashSet<>();

    protected boolean isSmallFile_;
    private final Set<CommAddress> confirmedReplicators_ = new HashSet<>();

    public WriteModuleVisitor() {
      state_ = STATE.INIT;
    }

    /**
     * Sends requests to store given object fragments.
     *
     * @param objectsMap
     * @param objectSize
     * @param previousVersionSHAs
     * @param isSmallFile
     * @param commitVersion
     *          Version of this commit. If version was not changed, it should be equal to null
     */
    protected void sendStoreQueries(Map<ObjectId, Map<CommAddress, EncryptedObject>> objectsMap,
        Map<ObjectId, Integer> objectSizesMap, Map<ObjectId, List<String>> previousVersionSHAsMap,
        boolean isSmallFile, Map<ObjectId, String> commitVersions) {
      state_ = STATE.REPLICA_UPDATE;
      isSmallFile_ = isSmallFile;
      objectSizesMap_ = objectSizesMap;

      for (Entry<ObjectId, Map<CommAddress, EncryptedObject>> entry : objectsMap.entrySet()) {
        ObjectId objectId = entry.getKey();
        for (Entry<CommAddress, EncryptedObject> placementEntry : entry.getValue().entrySet()) {
          CommAddress address = placementEntry.getKey();
          if (!objectsToSave_.containsKey(address)) {
            objectsToSave_.put(address, new HashMap<ObjectId, EncryptedObject>());
          }
          objectsToSave_.get(address).put(objectId, placementEntry.getValue());
        }
      }

      for (CommAddress address : objectsToSave_.keySet()) {
        String remoteJobId = CryptoUtils.getRandomId().toString();
        logger_.debug("Remote jobId for peer:" + address + " is: " + remoteJobId);
        logger_.debug("Sending objects of sizes: " + Maps.filterKeys(objectSizesMap_,
            Predicates.in(objectsToSave_.get(address).keySet())) +
            " to: " + address);
        waitingForTransactionResult_.put(address, remoteJobId);
        Set<ObjectId> objectIds = objectsToSave_.get(address).keySet();
        startSession(address, new StoreData(remoteJobId, objectsToSave_.get(address),
            filterKeys(previousVersionSHAsMap, objectIds),
            filterKeys(commitVersions, objectIds),
            filterKeys(objectSizesMap, objectIds)));
        logger_.info("added recipient: " + address);
      }
      recipientsSet_.addAll(objectsToSave_.keySet());
      timer_.schedule(jobId_, REPLICA_RESPONSE_TIMEOUT_MILIS);
    }

    public void visit(ConfirmationMessage message) {
      logger_.debug("received confirmation from " + message.getSourceAddress());
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        confirmedReplicators_.add(message.getSourceAddress());
        recipientsSet_.remove(message.getSourceAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(UpdateRejectMessage message) {
      logger_.debug("received updateRejectMessage");
      switch (state_) {
        case REPLICA_UPDATE :
          recipientsSet_.remove(message.getSourceAddress());
          sendTransactionAnswer(TransactionAnswer.ABORT);
          endModuleWithError(new NebuloException("Update failed due to inconsistent state."));
          break;
        case RETURNED_WAITING_FOR_REST :
          recipientsSet_.remove(message.getSourceAddress());
          waitingForTransactionResult_.remove(message.getDestinationAddress());
          rejectingOrWithholdingReplicators_.add(message.getSourceAddress());
          logger_.warn("Inconsitent state among replicas.");
          break;
        default :
          incorrectState(state_.name(), message);
      }
    }

    public void visit(UpdateWithholdMessage message) {
      logger_.debug("reject UpdateWithholdMessage");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        recipientsSet_.remove(message.getSourceAddress());
        waitingForTransactionResult_.remove(message.getDestinationAddress());
        rejectingOrWithholdingReplicators_.add(message.getSourceAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ErrorCommMessage message) {
      logger_.debug("received ErrorCommMessage");
      failReplicator(message.getMessage().getDestinationAddress(), message);
    }

    public void visit(TimeoutMessage message) {
      logger_.debug("Received TimeoutMessage");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        unavailableReplicators_.addAll(recipientsSet_);
        recipientsSet_.clear();
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ReplicatorErrorMessage message) {
      logger_.debug("Received ReplicatorErrorMessage from peer: " + message.getSourceAddress() +
          " with message: " + message.getMessage());
      failReplicator(message.getSourceAddress(), message);
    }

    private void failReplicator(CommAddress replicator, Message message) {
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        waitingForTransactionResult_.remove(replicator);
        recipientsSet_.remove(replicator);
        unavailableReplicators_.add(replicator);
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    private void tryReturnSemiResult() {
      logger_.debug("trying to return semi result");
      if (recipientsSet_.isEmpty() && !gotRequiredConfirmations(confirmedReplicators_)) {
        logger_.info("Cannot return semi result, aborting transaction");
        sendTransactionAnswer(TransactionAnswer.ABORT);
        endModuleWithError(new NebuloException("Not enough replicas responding to update file."));
      } else {
        if (!isSmallFile_) {
          logger_.debug("Big file");
          /*
           * big file - requires only CONFIRMATIONS_REQUIRED ConfirmationMessages, returns from
           * write and updates other replicas in background
           */
          if (gotRequiredConfirmations(confirmedReplicators_) && state_ == STATE.REPLICA_UPDATE) {
            logger_.debug("First type");
            logger_.debug("Query phase completed, waiting for result.");
            returnSemiResult(null);
            state_ = STATE.RETURNED_WAITING_FOR_REST;
          } else if (recipientsSet_.isEmpty()) {
            logger_.debug("Second type");
            logger_.debug("Query phase completed, waiting for result.");
            returnSemiResult(null);
          }
        } else {
          logger_.debug("Small file");
          if (recipientsSet_.isEmpty()) {
            logger_.debug("Query phase completed, waiting for result.");
            returnSemiResult(null);
          }
        }
      }
    }

    public void visit(TransactionAnswerInMessage message) {
      logger_.debug("received TransactionResult from parent");
      respondToAnswer(message);
      endModuleWithSuccess();
    }

    public void visit(InitSessionEndMessage message) {
      logger_.debug("Process " + message);
      StoreData storeData = (StoreData) message.getData();
      try {
        Map<ObjectId, EncryptedObject> encryptedObjects = new HashMap<>();
        for (Entry<ObjectId, EncryptedObject> objectEntry :
            storeData.getEncryptedObjects().entrySet()) {
          encryptedObjects.put(objectEntry.getKey(), encryption_.encryptWithSessionKey(
              objectEntry.getValue(), message.getSessionKey()));
        }
        networkQueue_.add(new QueryToStoreObjectsMessage(storeData.getRemoteJobId(),
            message.getPeerAddress(), encryptedObjects, storeData.getPreviousVersionSHAsMap(),
            getJobId(), storeData.getNewVersionSHAsMap(), message.getSessionId(),
            storeData.getObjectSizes()));
      } catch (CryptoException e) {
        endModuleWithError(e);
      }
    }

    public void visit(InitSessionEndWithErrorMessage message) {
      logger_.debug("Process InitSessionEndWithErrorMessage " + message);
      failReplicator(message.getPeerAddress(), message);
    }

    protected void respondToAnswer(TransactionAnswerInMessage message) {
      sendTransactionAnswer(message.answer_);
    }

    protected void sendTransactionAnswer(TransactionAnswer answer) {
      logger_.debug("sending transaction answer");
      for (Map.Entry<CommAddress, String> entry : waitingForTransactionResult_.entrySet()) {
        networkQueue_.add(new TransactionResultMessage(entry.getValue(), entry.getKey(), answer));
      }
    }

    // TODO(bolek): Maybe move it to a new superclass StateMachine?
    protected void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }

    private <K, V> Map<K, V> filterKeys(Map<K, V> toFilter, Set<K> allowedKeys) {
      return Maps.newHashMap(Maps.filterKeys(toFilter, Predicates.in(allowedKeys)));
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }

  /**
   * Just for readability - inner and private message in WriteNebuloObject.
   *
   * @author szymonmatejczyk
   */
  public static class TransactionAnswerInMessage extends Message {
    private static final long serialVersionUID = 3862738899180300188L;

    TransactionAnswer answer_;

    public TransactionAnswerInMessage(TransactionAnswer answer) {
      answer_ = answer;
    }
  }


  protected void startSession(CommAddress replicator, Serializable data) {
    outQueue_.add(new LocalInitSessionMessage(replicator, getJobId(), data));
  }

  @Override
  protected void performSecondPhase(TransactionAnswer answer) {
    logger_.debug("Performing second phase");
    inQueue_.add(new TransactionAnswerInMessage(answer));
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " with jobId_=" + jobId_;
  }
}
