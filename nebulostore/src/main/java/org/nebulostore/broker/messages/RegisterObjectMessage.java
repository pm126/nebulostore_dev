package org.nebulostore.broker.messages;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;

/**
 * @author Piotr Malicki
 */
public class RegisterObjectMessage extends Message {

  private static final long serialVersionUID = 6286313727498457780L;

  private final ObjectId objectId_;

  public RegisterObjectMessage(ObjectId objectId) {
    objectId_ = objectId;
  }

  public ObjectId getObjectId() {
    return objectId_;
  }

  @Override
  public JobModule getHandler() {
    return new BrokerMessageForwarder(this);
  }

}
