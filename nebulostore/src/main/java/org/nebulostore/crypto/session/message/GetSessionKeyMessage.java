package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorMessageForwarder;

/**
 * @author lukaszsiczek
 */
public class GetSessionKeyMessage extends Message {

  private static final long serialVersionUID = -7818319991343348884L;

  private final CommAddress peerAddress_;
  private final String sourceJobId_;
  private final String sessionId_;

  public GetSessionKeyMessage(CommAddress peerAddress, String sourceJobId, String sessionId) {
    peerAddress_ = peerAddress;
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  @Override
  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorMessageForwarder(this);
  }

}
