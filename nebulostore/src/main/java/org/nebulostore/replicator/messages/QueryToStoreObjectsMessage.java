package org.nebulostore.replicator.messages;

import java.util.List;
import java.util.Map;

import com.rits.cloning.Cloner;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;

/**
 * @author szymonmatejczyk
 * This is a query to store a particular object.
 */
public class QueryToStoreObjectsMessage extends InReplicatorMessage
    implements SessionInnerMessageInterface {
  private static final long serialVersionUID = 3283983404037381657L;

  private final Map<ObjectId, EncryptedObject> encryptedEntities_;
  private final Map<ObjectId, List<String>> previousVersionSHAsMap_;
  private final Map<ObjectId, String> newVersionSHAsMap_;
  private final Map<ObjectId, Integer> fullObjectSizes_;
  private final String sourceJobId_;
  private final String sessionId_;

  public QueryToStoreObjectsMessage(String jobId, CommAddress destAddress,
      Map<ObjectId, EncryptedObject> encryptedEntities,
      Map<ObjectId, List<String>> previousVersionSHAsMap, String sourceJobId,
      Map<ObjectId, String> newVersionSHAsMap, String sessionId,
      Map<ObjectId, Integer> fullObjectSizes) {
    super(jobId, destAddress);
    encryptedEntities_ = encryptedEntities;
    Cloner c = new Cloner();
    previousVersionSHAsMap_ = c.deepClone(previousVersionSHAsMap);
    sourceJobId_ = sourceJobId;
    newVersionSHAsMap_ = newVersionSHAsMap;
    sessionId_ = sessionId;
    fullObjectSizes_ = fullObjectSizes;
  }

  public Map<ObjectId, EncryptedObject> getEncryptedEntities() {
    return encryptedEntities_;
  }

  @Override
  public String getSourceJobId() {
    return sourceJobId_;
  }

  public Map<ObjectId, List<String>> getPreviousVersionSHAsMap() {
    return previousVersionSHAsMap_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  public Map<ObjectId, String> getNewVersionSHAsMap() {
    return newVersionSHAsMap_;
  }

  public Map<ObjectId, Integer> getFullObjectSizes() {
    return fullObjectSizes_;
  }

  @Override
  public String toString() {
    return super.toString() + ", " + this.getClass().getSimpleName() + " { previousVersionSHAs=" +
        previousVersionSHAsMap_ + ", sourceJobId=" + sourceJobId_ +
        ", newVersionSHA=" + newVersionSHAsMap_ + ", sessionId=" + sessionId_ +
        ", fullObjectSize=" + fullObjectSizes_;
  }
}
