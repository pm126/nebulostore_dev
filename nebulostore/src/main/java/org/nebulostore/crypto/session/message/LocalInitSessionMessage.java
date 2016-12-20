package org.nebulostore.crypto.session.message;

import java.io.Serializable;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorMessageForwarder;

public class LocalInitSessionMessage extends Message {

  private static final long serialVersionUID = 2270173910332585452L;

  private final CommAddress peerAddress_;
  private final String localSourceJobId_;
  private final Serializable data_;

  public LocalInitSessionMessage(CommAddress peerAddress, String localSourceJobId,
      Serializable data) {
    peerAddress_ = peerAddress;
    localSourceJobId_ = localSourceJobId;
    data_ = data;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public String getLocalSourceJobId() {
    return localSourceJobId_;
  }

  public Serializable getData() {
    return data_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new InitSessionNegotiatorMessageForwarder(this);
  }

  @Override
  public String toString() {
    return "{" + getClass().getSimpleName() + ": peerAddress=" + peerAddress_ +
        ", localSourceJobId_=" + localSourceJobId_ + ", data: " + data_ + "; " +
        super.toString() + "}";
  }

}
