package org.nebulostore.replicator.messages;

import java.util.List;
import java.util.Map;

import com.rits.cloning.Cloner;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * This is a message containing requested object.
 * @author Bolek Kulbabinski
 */
public class SendObjectsMessage extends OutReplicatorMessage {
  private static final long serialVersionUID = 5852937000391705084L;

  private final String sessionId_;

  private final Map<ObjectId, EncryptedObject> encryptedEntities_;
  private final Map<ObjectId, List<String>> versionsMap_;
  private final Map<ObjectId, Integer> objectsSizes_;
  private final String requestId_;

  public SendObjectsMessage(String jobId, CommAddress destAddress, String sessionId,
      Map<ObjectId, EncryptedObject> encryptedObjects, Map<ObjectId, List<String>> versionsMap,
      Map<ObjectId, Integer> objectsSizes, String requestId) {
    super(jobId, destAddress);
    encryptedEntities_ = encryptedObjects;
    sessionId_ = sessionId;
    Cloner c = new Cloner();
    versionsMap_ = c.deepClone(versionsMap);
    objectsSizes_ = objectsSizes;
    requestId_ = requestId;
  }

  public Map<ObjectId, EncryptedObject> getEncryptedEntities() {
    return encryptedEntities_;
  }

  public Map<ObjectId, List<String>> getVersionsMap() {
    return versionsMap_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  public Map<ObjectId, Integer> getObjectsSizes() {
    return objectsSizes_;
  }

  public String getRequestId() {
    return requestId_;
  }

  @Override
  public String toString() {
    return "{ SendObjectMessage { " + super.toString() + ", sessionId_=" + sessionId_ +
        ", versionsMap_=" + versionsMap_ + ", objectsSizes_=" + objectsSizes_ + " } }";
  }
}
