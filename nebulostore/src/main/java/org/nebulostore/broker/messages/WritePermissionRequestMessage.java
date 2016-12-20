package org.nebulostore.broker.messages;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;

/**
 * @author Piotr Malicki
 */
public class WritePermissionRequestMessage extends Message {

  private static final long serialVersionUID = 2510284941587013728L;

  private final String writeModuleJobId_;
  private final ObjectId objectId_;

  public WritePermissionRequestMessage(String writeModuleJobId, ObjectId objectId) {
    writeModuleJobId_ = writeModuleJobId;
    objectId_ = objectId;
  }

  public String getWriteModuleJobId() {
    return writeModuleJobId_;
  }

  public ObjectId getObjectId() {
    return objectId_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }
}
