package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public abstract class SessionCryptoMessage extends CommMessage {

  private static final long serialVersionUID = -920124952136609208L;

  private final EncryptedObject data_;
  private final String sourceJobId_;
  private final String sessionId_;

  public SessionCryptoMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress,
      String sessionId, String sourceJobId, EncryptedObject data) {
    super(jobId, sourceAddress, destAddress);
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
    data_ = data;
  }

  public SessionCryptoMessage(CommAddress sourceAddress, CommAddress destAddress,
      String sessionId, String sourceJobId, EncryptedObject data) {
    super(sourceAddress, destAddress);
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
    data_ = data;
  }

  public EncryptedObject getEncryptedData() {
    return data_;
  }

  @Override
  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  @Override
  public String toString() {
    return "{SessionCryptoMessage: " + super.toString() + ", peerAddress_= " +
        getDestinationAddress() + ", sessionId_= " + sessionId_ + ", sourceJobId_ =" +
        sourceJobId_ + "}";
  }
}
