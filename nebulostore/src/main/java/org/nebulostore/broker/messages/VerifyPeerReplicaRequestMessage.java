package org.nebulostore.broker.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.replicator.messages.QueryToStoreObjectsMessage;

public class VerifyPeerReplicaRequestMessage extends Message {

  private static final long serialVersionUID = 5484116142626194057L;
  private String sourceJobId_;
  private QueryToStoreObjectsMessage message_;

  public VerifyPeerReplicaRequestMessage(String sourceJobId,
      QueryToStoreObjectsMessage message) {
    super();
    sourceJobId_ = sourceJobId;
    message_ = message;
  }

  public VerifyPeerReplicaResponseMessage getResponse(boolean decision) {
    return new VerifyPeerReplicaResponseMessage(sourceJobId_, decision, message_);
  }

  public QueryToStoreObjectsMessage getVerifyingMessage() {
    return message_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }
}
