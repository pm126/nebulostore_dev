package org.nebulostore.dfuntest.async;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.communication.naming.CommAddress;

public class AsyncTestAsynchronousMessage extends AsynchronousMessage {

  private static final long serialVersionUID = -6598662770230745179L;

  private final CommAddress recipient_;
  private final String originalMessageId_;
  private final int receiverLoginCount_;

  public AsyncTestAsynchronousMessage(CommAddress recipient, String originalMessageId,
      int receiverLoginCount) {
    recipient_ = recipient;
    originalMessageId_ = originalMessageId;
    receiverLoginCount_ = receiverLoginCount;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  public String getOriginalMessageId() {
    return originalMessageId_;
  }

  @Override
  public JobModule getHandler() {
    return new AsyncTestHelperMessageForwarder(this);
  }

  public int getReceiverLoginCount() {
    return receiverLoginCount_;
  }

  @Override
  public String toString() {
    return "AsyncTestAsynchronousMessage: { id: " + getMessageId() + ", original message id: " +
        originalMessageId_ + "}";
  }

}
