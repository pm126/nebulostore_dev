package org.nebulostore.dfuntest.coding.communication.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.replicator.messages.SendObjectsMessage;

public class SendObjectMessageWithSize extends CommMessage {

  private static final long serialVersionUID = 5628361727297471144L;

  private final SendObjectsMessage message_;
  private final int size_;

  public SendObjectMessageWithSize(SendObjectsMessage message, int size) {
    super(message.getId(), message.getSourceAddress(), message.getDestinationAddress());
    message_ = message;
    size_ = size;
  }

  public SendObjectsMessage getMessage() {
    return message_;
  }

  public int getSize() {
    return size_;
  }

  @Override
  public String toString() {
    return "{ SendObjectMessageWithSize { " + super.toString() + ", message_=" + message_ +
        ", size_=" + size_ + "} }";
  }

}
