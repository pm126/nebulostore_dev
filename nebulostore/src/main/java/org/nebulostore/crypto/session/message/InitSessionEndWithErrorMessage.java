package org.nebulostore.crypto.session.message;

import java.io.Serializable;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionEndWithErrorMessage extends Message {

  private static final long serialVersionUID = 6029715086748333997L;

  private final String errorMessage_;
  private final CommAddress peerAddress_;
  private final Serializable data_;

  public InitSessionEndWithErrorMessage(String jobId, String errorMessage,
      CommAddress peerAddress, Serializable data) {
    super(jobId);
    errorMessage_ = errorMessage;
    peerAddress_ = peerAddress;
    data_ = data;
  }

  public String getErrorMessage() {
    return errorMessage_;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public Serializable getData() {
    return data_;
  }

  @Override
  public String toString() {
    return "{" + getClass().getSimpleName() + ": errorMessage_=" + errorMessage_ +
        ", peerAddress_=" + peerAddress_ + ", data_=" + data_ + "; " + super.toString() + "}";
  }
}
