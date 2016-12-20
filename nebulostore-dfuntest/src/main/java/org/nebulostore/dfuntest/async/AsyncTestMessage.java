package org.nebulostore.dfuntest.async;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.errorresponder.ErrorResponder;
import org.nebulostore.communication.routing.errorresponder.SendAsyncMessageErrorResponder;

public class AsyncTestMessage extends CommMessage {

  private static final long serialVersionUID = -6796667119053107754L;

  private final int receiverLoginCount_;

  public AsyncTestMessage(CommAddress sourceAddress, CommAddress destAddress,
      int receiverLoginCount) {
    super(sourceAddress, destAddress);
    receiverLoginCount_ = receiverLoginCount;
  }

  @Override
  public ErrorResponder generateErrorResponder(BlockingQueue<Message> dispatcherQueue) {
    return new SendAsyncMessageErrorResponder(new AsyncTestAsynchronousMessage(
        getDestinationAddress(), getMessageId(), receiverLoginCount_), getDestinationAddress(),
        dispatcherQueue);
  }

  @Override
  public boolean requiresAck() {
    return true;
  }

  public int getReceiverLoginCount() {
    return receiverLoginCount_;
  }

  @Override
  public JobModule getHandler() {
    return new AsyncTestHelperMessageForwarder(this);
  }

  @Override
  public String toString() {
    return super.toString() + " with loginCount = " + receiverLoginCount_;
  }
}
