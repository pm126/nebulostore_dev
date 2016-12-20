package org.nebulostore.dfuntest.coding.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.dfuntest.coding.CodingTestHelperMessageForwarder;

public class RecreationSentDataSizeMessage extends Message {

  private static final long serialVersionUID = -2146741654387156595L;

  private final int size_;
  private final int numberOfObjects_;

  public RecreationSentDataSizeMessage(int size, int numberOfObjects) {
    size_ = size;
    numberOfObjects_ = numberOfObjects;
  }

  public int getSize() {
    return size_;
  }
  public int getNumberOfObjects() {
    return numberOfObjects_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new CodingTestHelperMessageForwarder(this);
  }
}
