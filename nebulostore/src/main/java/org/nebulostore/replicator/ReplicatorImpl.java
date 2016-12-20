package org.nebulostore.replicator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetModule.GetModuleMode;
import org.nebulostore.api.RecreateObjectFragmentsModule;
import org.nebulostore.api.RecreateObjectFragmentsModule.FragmentsData;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.async.messages.UpdateNebuloObjectMessage;
import org.nebulostore.broker.messages.CheckContractMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.message.GetSessionKeyMessage;
import org.nebulostore.crypto.session.message.GetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.DeleteObjectException;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.core.StoreData;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.CheckContractResultMessage;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.DeleteObjectMessage;
import org.nebulostore.replicator.messages.GetObjectsMessage;
import org.nebulostore.replicator.messages.ObjectOutdatedMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectsMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectsMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;
import org.nebulostore.replicator.messages.UpdateRejectMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage.Reason;
import org.nebulostore.utils.LockMap;

/**
 * Replicator - disk interface.
 *
 * @author szymonmatejczyk
 * @author Bolek Kulbabinski
 */
public class ReplicatorImpl extends Replicator {

  private static Logger logger_ = Logger.getLogger(ReplicatorImpl.class);

  private static final int UPDATE_TIMEOUT_SEC = 30;
  private static final int LOCK_TIMEOUT_SEC = 30;
  private static final int GET_OBJECT_TIMEOUT_SEC = 30;

  private static final String METADATA_SUFFIX = ".meta";
  private static final String TMP_SUFFIX = ".tmp.";
  private static final String INDEX_ID = "object.index";

  private enum ActionType { WRITE, READ }

  private static LockMap lockMap_ = new LockMap();

  private final KeyValueStore<byte[]> store_;
  private final MessageVisitor visitor_ = new ReplicatorVisitor();
  private EncryptionAPI encryptionAPI_;
  private final Map<String, SessionInnerMessageInterface> workingMessages_ =
      new HashMap<String, SessionInnerMessageInterface>();
  private final Map<String, SecretKey> workingSecretKeys_ =
      new HashMap<String, SecretKey>();
  private final Map<String, ActionType> actionTypes_ = new HashMap<String, ActionType>();
  private Provider<RecreateObjectFragmentsModule> getFragmentModuleProvider_;
  private CommAddress myAddress_;


  @Inject
  public ReplicatorImpl(@Named("ReplicatorStore") KeyValueStore<byte[]> store,
      EncryptionAPI encryptionAPI,
      Provider<RecreateObjectFragmentsModule> getFragmentModuleProvider,
      CommAddress myAddress) {
    store_ = store;
    encryptionAPI_ = encryptionAPI;
    getFragmentModuleProvider_ = getFragmentModuleProvider;
    myAddress_ = myAddress;
  }

  public ReplicatorImpl(KeyValueStore<byte[]> store) {
    store_ = store;
  }

  @Override
  public void cacheStoredObjectsMeta() {
    storedObjectsMeta_ = getOrCreateStoredObjectsIndex(store_);
  }

  private static Map<String, MetaData> getOrCreateStoredObjectsIndex(KeyValueStore<byte[]> store) {
    try {
      final byte[] empty = toJson(new HashMap<String, MetaData>());
      store.performTransaction(INDEX_ID, new Function<byte[], byte[]>() {
        @Override
        public byte[] apply(byte[] existing) {
          return existing == null ? empty : existing;
        }
      });
    } catch (IOException e) {
      logger_.error("Could not initialize index!", e);
      return new HashMap<>();
    }
    return fromJson(store.get(INDEX_ID));
  }

  private static byte[] toJson(Map<String, MetaData> set) {
    Gson gson = new Gson();
    return gson.toJson(set).getBytes(Charsets.UTF_8);
  }

  private static Map<String, MetaData> fromJson(byte[] json) {
    Gson gson = new Gson();
    return gson.fromJson(new String(json, Charsets.UTF_8),
        new TypeToken<Map<String, MetaData>>() { } .getType());
  }

  /**
   * Result of queryToStore.
   */
  private enum QueryToStoreResult { OK, OBJECT_OUT_OF_DATE, INVALID_VERSION, SAVE_FAILED, TIMEOUT }

  /**
   * Visitor to handle different message types.
   * @author szymonmatejczyk
   */
  protected class ReplicatorVisitor extends MessageVisitor {
    private StoreData storeData_;

    private void saveObjects(SecretKey sessionKey, QueryToStoreObjectsMessage message)
        throws NebuloException {
      Set<ObjectId> storedObjects = new HashSet<>();
      Map<ObjectId, EncryptedObject> decryptedEntities = new HashMap<>();
      for (ObjectId objectId : message.getEncryptedEntities().keySet()) {
        EncryptedObject enc = null;
        try {
          enc = (EncryptedObject) encryptionAPI_.decryptWithSessionKey(
              message.getEncryptedEntities().get(objectId), sessionKey);
        } catch (NullPointerException | CryptoException e) {
          revertWrites(storedObjects, message.getId());
          dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
              message.getSourceAddress(), "Unable to save object with id: " + objectId);
          return;

        }
        decryptedEntities.put(objectId, enc);

        QueryToStoreResult result = queryToUpdateObject(objectId,
            enc, message.getPreviousVersionSHAsMap().get(objectId), message.getId());
        logger_.debug("queryToUpdateObject returned " + result.name());
        switch (result) {
          case OK:
            storedObjects.add(objectId);
            break;
          case OBJECT_OUT_OF_DATE:
            networkQueue_.add(new UpdateWithholdMessage(message.getSourceJobId(),
                message.getSourceAddress(), Reason.OBJECT_OUT_OF_DATE));
            revertWrites(storedObjects, message.getId());
            endJobModule();
            return;
          case INVALID_VERSION:
            networkQueue_.add(new UpdateRejectMessage(message.getSourceJobId(),
                message.getSourceAddress()));
            revertWrites(storedObjects, message.getId());
            endJobModule();
            return;
          case SAVE_FAILED:
            networkQueue_.add(new UpdateWithholdMessage(message.getSourceJobId(),
                message.getSourceAddress(), Reason.SAVE_FAILURE));
            revertWrites(storedObjects, message.getId());
            endJobModule();
            return;
          case TIMEOUT:
            networkQueue_.add(new UpdateWithholdMessage(message.getSourceJobId(),
                message.getSourceAddress(), Reason.TIMEOUT));
            revertWrites(storedObjects, message.getId());
            endJobModule();
            return;
          default:
            break;
        }
      }
      networkQueue_.add(new ConfirmationMessage(message.getSourceJobId(),
          message.getSourceAddress()));
      storeData_ = new StoreData(message.getId(), decryptedEntities,
          message.getPreviousVersionSHAsMap(), message.getNewVersionSHAsMap(),
          message.getFullObjectSizes());
      try {
        TransactionResultMessage m = (TransactionResultMessage) inQueue_.poll(
            LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (m == null) {
          revertWrites(storedObjects, message.getId());
          logger_.warn("Transaction aborted - timeout.");
        } else {
          processMessage(m);
        }
      } catch (InterruptedException exception) {
        revertWrites(storedObjects, message.getId());
        throw new NebuloException("Timeout while handling QueryToStoreObjectMessage",
            exception);
      } catch (ClassCastException exception) {
        revertWrites(storedObjects, message.getId());
        throw new NebuloException("Wrong message type received.", exception);
      }
      endJobModule();
    }

    public void visit(QueryToStoreObjectsMessage message) throws NebuloException {
      logger_.debug("StoreObjectMessage received");
      cacheStoredObjectsMeta();
      CommAddress peerAddress = message.getSourceAddress();
      workingMessages_.put(message.getSessionId(), message);
      actionTypes_.put(message.getSessionId(), ActionType.WRITE);
      outQueue_.add(new GetSessionKeyMessage(peerAddress, getJobId(),
          message.getSessionId()));
    }

    public void visit(TransactionResultMessage message) {
      logger_.debug("TransactionResultMessage received: " + message.getResult());
      if (storeData_ == null) {
        //TODO(szm): ignore late abort transaction messages send by timer.
        logger_.warn("Unexpected commit message received.");
        endJobModule();
        return;
      }
      if (message.getResult() == TransactionAnswer.COMMIT) {
        for (Entry<ObjectId, EncryptedObject> entry : storeData_.getEncryptedObjects().entrySet()) {
          ObjectId objectId = entry.getKey();
          commitUpdateObject(objectId,
              storeData_.getPreviousVersionSHAsMap().get(objectId),
              storeData_.getNewVersionSHAsMap().get(objectId),
              message.getId(),
              storeData_.getObjectSizes().get(objectId));
        }
      } else {
        revertWrites(storeData_.getEncryptedObjects().keySet(), message.getId());
      }
      endJobModule();
    }

    public void visit(GetObjectsMessage message) {
      cacheStoredObjectsMeta();
      CommAddress peerAddress = message.getSourceAddress();
      workingMessages_.put(message.getSessionId(), message);
      actionTypes_.put(message.getSessionId(), ActionType.READ);
      outQueue_.add(new GetSessionKeyMessage(peerAddress, getJobId(),
          message.getSessionId()));
    }

    public void visit(GetSessionKeyResponseMessage message) throws NebuloException {
      workingSecretKeys_.put(message.getSessionId(), message.getSessionKey());
      String sessionId = message.getSessionId();
      if (actionTypes_.get(sessionId).equals(ActionType.WRITE)) {
        outQueue_.add(new CheckContractMessage(getJobId(), message.getPeerAddress(),
            message.getSessionId()));
      } else {
        SessionInnerMessageInterface insideSessionMessage = workingMessages_.remove(sessionId);
        CommAddress peerAddress = insideSessionMessage.getSourceAddress();
        performAction(sessionId, insideSessionMessage, peerAddress);
      }
    }

    public void visit(InitSessionEndWithErrorMessage message) {
      logger_.debug("InitSessionEndWithErrorMessage " + message.getErrorMessage());
      SessionInnerMessageInterface getObjectMessage =
          workingMessages_.remove(message.getPeerAddress());
      dieWithError(getObjectMessage.getSourceJobId(), getObjectMessage.getDestinationAddress(),
          message.getPeerAddress(), "Unable to retrieve object.");
    }

    private void retrieveObjects(CommAddress peerAddress, String sessionId,
        SecretKey sessionKey, GetObjectsMessage getObjectsMessage) {

      Map<ObjectId, List<String>> versionsMap = null;
      try {
        versionsMap =
            getPreviousVersions(getObjectsMessage.getObjectIds());
      } catch (IOException e) {
        logger_.debug("Exception when getting previous versions ", e);
        dieWithError(getObjectsMessage.getSourceJobId(), getObjectsMessage.getDestinationAddress(),
            peerAddress, "Unable to retrieve object.", getObjectsMessage.getRequestId());
        return;
      }

      Map<ObjectId, EncryptedObject> encMap = new HashMap<>();
      try {
        Map<ObjectId, EncryptedObject> objectsMap = getObjects(getObjectsMessage.getObjectIds());
        EncryptedObject enc = null;
        for (Entry<ObjectId, EncryptedObject> entry : objectsMap.entrySet()) {
          enc = encryptionAPI_.encryptWithSessionKey(entry.getValue(), sessionKey);
          encMap.put(entry.getKey(), enc);
        }
      } catch (NullPointerException | CryptoException e) {
        logger_.debug("Encryption with session key failed!", e);
        dieWithError(getObjectsMessage.getSourceJobId(),
            getObjectsMessage.getDestinationAddress(),
            peerAddress, "Unable to retrieve object.",
            getObjectsMessage.getRequestId());
        return;
      }

      Map<ObjectId, Integer> objectsSizes = new HashMap<>();
      for (ObjectId objectId : getObjectsMessage.getObjectIds()) {
        objectsSizes.put(objectId,
            storedObjectsMeta_.get(objectId.toString()).getWholeObjectSize());
      }

      logger_.debug("Source job id: " + getObjectsMessage.getSourceJobId());
      logger_.debug("Object ids: " + getObjectsMessage.getObjectIds());
      logger_.debug("Metadata: " + storedObjectsMeta_.get(
          getObjectsMessage.getObjectIds().toString()));
      networkQueue_.add(new SendObjectsMessage(getObjectsMessage.getSourceJobId(), peerAddress,
          sessionId, encMap, versionsMap, objectsSizes, getObjectsMessage.getRequestId()));
      endJobModule();
    }

    public void visit(CheckContractResultMessage message) throws NebuloException {
      String sessionId = message.getSessionId();
      SessionInnerMessageInterface insideSessionMessage = workingMessages_.remove(sessionId);
      CommAddress peerAddress = insideSessionMessage.getSourceAddress();
      if (!message.getResult()) {
        dieWithError(insideSessionMessage.getSourceJobId(),
            insideSessionMessage.getDestinationAddress(), peerAddress, "CheckContract error.");
        return;
      }
      performAction(sessionId, insideSessionMessage, peerAddress);
    }

    public void visit(DeleteObjectMessage message) {
      try {
        deleteObject(message.getObjectId());
        networkQueue_.add(new ConfirmationMessage(message.getSourceJobId(),
            message.getSourceAddress()));
      } catch (DeleteObjectException exception) {
        logger_.warn(exception.toString());
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), exception.getMessage());
      }
      endJobModule();
    }

    public void visit(ObjectOutdatedMessage message) {
      updateOutdatedObject(message.getAddress(), message.getId());
    }

    public void visit(UpdateNebuloObjectMessage message) {
      updateOutdatedObject(message.getObjectAddress(), message.getId());
    }

    private void performAction(String sessionId, SessionInnerMessageInterface insideSessionMessage,
        CommAddress peerAddress) throws NebuloException {
      logger_.debug("Performing action with parameters: sessionId=" + sessionId +
          ", insideSessionMessage=" + insideSessionMessage + ", peerAddress=" + peerAddress);
      SecretKey sessionKey = workingSecretKeys_.remove(sessionId);
      ActionType actionType = actionTypes_.remove(sessionId);
      logger_.debug("CheckContractResultMessage Peer " + peerAddress);

      switch (actionType) {
        case READ:
          retrieveObjects(peerAddress, sessionId, sessionKey,
              (GetObjectsMessage) insideSessionMessage);
          break;
        case WRITE:
          saveObjects(sessionKey, (QueryToStoreObjectsMessage) insideSessionMessage);
          break;
        default:
          break;
      }
    }

    private void dieWithError(String jobId, CommAddress sourceAddress,
        CommAddress destinationAddress, String errorMessage) {
      dieWithError(jobId, sourceAddress, destinationAddress, errorMessage, null);
    }

    private void dieWithError(String jobId, CommAddress sourceAddress,
        CommAddress destinationAddress, String errorMessage, String requestId) {
      networkQueue_.add(new ReplicatorErrorMessage(jobId, destinationAddress, errorMessage,
          requestId));
      endJobModule();
    }

    private void revertWrites(Set<ObjectId> objectIds, String transactionToken) {
      for (ObjectId objectId : objectIds) {
        abortUpdateObject(objectId, transactionToken);
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Begins transaction: tries to store object to temporal location.
   */
  public QueryToStoreResult queryToUpdateObject(ObjectId objectId,
      EncryptedObject encryptedObject, List<String> previousVersions, String transactionToken) {
    logger_.debug("Checking store consistency");
    logger_.debug("Storing encrypted object with id=" + objectId +
        ", previousVersions=" + previousVersions);
    try {
      if (!lockMap_.tryLock(objectId.toString(), UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Object " + objectId + " lock timeout in queryToUpdateObject().");
        return QueryToStoreResult.TIMEOUT;
      }
    } catch (InterruptedException exception) {
      logger_.warn("Interrupted while waiting for object lock in queryToUpdateObject()");
      return QueryToStoreResult.TIMEOUT;
    }

    try {
      byte[] metaData = store_.get(objectId.toString() + METADATA_SUFFIX);
      if (metaData != null) {
        List<String> localPreviousVersions =
            getPreviousVersions(Sets.newHashSet(objectId)).get(objectId);

        // checking whether remote file is up to date (update is not concurrent)
        if (!previousVersions.containsAll(localPreviousVersions)) {
          lockMap_.unlock(objectId.toString());
          return QueryToStoreResult.INVALID_VERSION;
        }

        // checking whether local file is up to date
        if (!localPreviousVersions.containsAll(previousVersions)) {
          lockMap_.unlock(objectId.toString());
          return QueryToStoreResult.OBJECT_OUT_OF_DATE;
        }
      } else {
        logger_.debug("storing new file");
      }

      String tmpKey = objectId.toString() + TMP_SUFFIX + transactionToken;
      store_.put(tmpKey, encryptedObject.getEncryptedData());
      return QueryToStoreResult.OK;
    } catch (IOException e) {
      lockMap_.unlock(objectId.toString());
      return QueryToStoreResult.SAVE_FAILED;
    }
  }

  public void commitUpdateObject(ObjectId objectId, List<String> previousVersions,
      String currentVersion, String transactionToken, int objectSize) {
    logger_.debug("Commit storing object " + objectId.toString());

    try {
      String objId = objectId.toString();
      String tmpKey = objId + TMP_SUFFIX + transactionToken;
      byte[] bytes = store_.get(tmpKey);
      store_.delete(tmpKey);
      store_.put(objId, bytes);
      addToIndex(new MetaData(objId, objectSize));

      List<String> newVersions = new LinkedList<String>(previousVersions);
      if (currentVersion != null) {
        newVersions.add(currentVersion);
      }
      setPreviousVersions(objectId, newVersions);

      logger_.debug("Commit successful");
    } catch (IOException e) {
      // TODO: dirty state here
      logger_.warn("unable to save file", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  public void abortUpdateObject(ObjectId objectId, String transactionToken) {
    logger_.debug("Aborting transaction " + objectId.toString());
    try {
      String tmpKey = objectId.toString() + TMP_SUFFIX + transactionToken;
      store_.delete(tmpKey);
    } catch (IOException e) {
      // TODO: dirty state here
      logger_.warn("unable to delete file", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  private void updateOutdatedObject(NebuloAddress address, String transactionToken) {
    try {
      RecreateObjectFragmentsModule getModule = getFragmentModuleProvider_.get();
      getModule.getAndCalcFragments(Sets.newHashSet(address), myAddress_, false,
          GetModuleMode.FLEXIBLE);
      FragmentsData fragmentsData = getModule.getResult(GET_OBJECT_TIMEOUT_SEC);
      EncryptedObject encryptedObject = fragmentsData.fragmentsMap_.
          get(address.getObjectId());
      try {
        deleteObject(address.getObjectId());
      } catch (DeleteObjectException exception) {
        logger_.warn("Error deleting file.");
      }

      QueryToStoreResult query = queryToUpdateObject(address.getObjectId(),
          encryptedObject, fragmentsData.versionsMap_.get(address.getObjectId()), transactionToken);
      if (query == QueryToStoreResult.OK || query == QueryToStoreResult.OBJECT_OUT_OF_DATE ||
          query == QueryToStoreResult.INVALID_VERSION) {
        commitUpdateObject(address.getObjectId(), fragmentsData.versionsMap_.
            get(address.getObjectId()), null, transactionToken,
            fragmentsData.objectsSizes_.get(address.getObjectId()));
      } else {
        throw new NebuloException("Unable to fetch new version of file.");
      }
    } catch (NebuloException exception) {
      logger_.warn(exception);
    }
  }

  private void addToIndex(final MetaData metaData) {
    logger_.debug("Adding to index: " + metaData);
    transformIndex(new Function<byte[], byte[]>() {

      @Override
      public byte[] apply(byte[] index) {
        storedObjectsMeta_ = fromJson(index);
        storedObjectsMeta_.put(metaData.getObjectId(), metaData);
        return toJson(storedObjectsMeta_);
      }
    });
    logger_.debug("current index: " + storedObjectsMeta_);
  }

  private void removeFromIndexbyId(final String objId) {
    logger_.debug("Removing from index: " + objId);
    transformIndex(new Function<byte[], byte[]>() {

      @Override
      public byte[] apply(byte[] index) {
        storedObjectsMeta_ = fromJson(index);
        storedObjectsMeta_.remove(objId);
        return toJson(storedObjectsMeta_);
      }
    });
    logger_.debug("Current index: " + storedObjectsMeta_);
  }

  private void transformIndex(Function<byte[], byte[]> function) {
    try {
      store_.performTransaction(INDEX_ID, function);
    } catch (IOException e) {
      logger_.error("Could not save index!", e);
    }
  }

  /**
   * Retrieves objects from disk.
   * @return Encrypted object or null if and only if object can't be read from disk(either because
   * it wasn't stored or there was a problem reading file).
   */
  private Map<ObjectId, EncryptedObject> getObjects(Set<ObjectId> objectIds) {
    logger_.debug("getObject with objectID = " + objectIds);
    Map<ObjectId, EncryptedObject> result = new HashMap<>();
    for (ObjectId objectId : objectIds) {
      byte[] bytes = store_.get(objectId.toString());

      if (bytes == null) {
        throw new NullPointerException();
      } else {
        result.put(objectId, new EncryptedObject(bytes));
      }
    }

    return result;
  }

  public void deleteObject(ObjectId objectId) throws DeleteObjectException {
    try {
      if (!lockMap_.tryLock(objectId.toString(), UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Object " + objectId.toString() + " lock timeout in deleteObject().");
        throw new DeleteObjectException("Timeout while waiting for object lock.");
      }
    } catch (InterruptedException e) {
      logger_.warn("Interrupted while waiting for object lock in deleteObject()");
      throw new DeleteObjectException("Interrupted while waiting for object lock.", e);
    }

    try {
      store_.delete(objectId.toString());
      store_.delete(objectId.toString() + METADATA_SUFFIX);
      removeFromIndexbyId(objectId.toString());
    } catch (IOException e) {
      throw new DeleteObjectException("Unable to delete file.", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  private Map<ObjectId, List<String>> getPreviousVersions(Set<ObjectId> objectIds)
      throws IOException {
    Map<ObjectId, List<String>> results = new HashMap<>();
    for (ObjectId objectId : objectIds) {
      byte[] bytes = store_.get(objectId.toString() + METADATA_SUFFIX);
      if (bytes == null) {
        return null;
      } else {
        logger_.debug("Downloaded previous versions of object " + objectId + ": " +
            new String(bytes, Charsets.UTF_8));
        results.put(objectId, Lists.newLinkedList(
            Splitter.on(",").split(new String(bytes, Charsets.UTF_8))));
      }
    }
    return results;
  }

  private void setPreviousVersions(ObjectId objectId, List<String> versions) throws IOException {
    logger_.debug("Setting previous version for object " + objectId + " to the list: " +
        versions);
    String joined = Joiner.on(",").join(versions);
    store_.put(objectId.toString() + METADATA_SUFFIX, joined.getBytes(Charsets.UTF_8));
  }
}
