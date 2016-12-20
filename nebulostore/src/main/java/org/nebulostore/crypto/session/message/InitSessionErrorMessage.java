package org.nebulostore.crypto.session.message;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionErrorMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = 6523437594110697492L;

  public String errorMessage_;

  public InitSessionErrorMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, String errorMessage, String sessionId) {
    super(jobId, sourceAddress, destAddress, sessionId, null, null);
    errorMessage_ = errorMessage;
  }

  public String getErrorMessage() {
    return errorMessage_;
  }

  @Override
  public String toString() {
    return " {" + getClass().getSimpleName() + "; " + super.toString() + ", errorMessage_=" +
        errorMessage_ + "}";
  }
}
