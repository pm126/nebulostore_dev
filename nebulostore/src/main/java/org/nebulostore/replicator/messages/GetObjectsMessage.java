package org.nebulostore.replicator.messages;

import java.util.Set;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;

/**
 * This is a request for a set of particular objects sent to peer that hold this
 * object's replica. Sender waits for a response wrapped into SendObjectMessage.
 * @author Bolek Kulbabinski
 */
public class GetObjectsMessage extends InReplicatorMessage implements SessionInnerMessageInterface {
  private static final long serialVersionUID = 1660420694986822395L;

  private final Set<ObjectId> objectIds_;
  private final String sourceJobId_;
  private String sessionId_;
  private String requestId_;

  public GetObjectsMessage(CommAddress destAddress, Set<ObjectId> objectIds, String sourceJobId) {
    super(destAddress);
    objectIds_ = objectIds;
    sourceJobId_ = sourceJobId;
  }

  public GetObjectsMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress,
      Set<ObjectId> objectIds, String sourceJobId, String sessionId, String requestId) {
    super(jobId, sourceAddress, destAddress);
    objectIds_ = objectIds;
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
    requestId_ = requestId;
  }

  public Set<ObjectId> getObjectIds() {
    return objectIds_;
  }

  @Override
  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  public String getRequestId() {
    return requestId_;
  }

}
