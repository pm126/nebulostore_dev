package org.nebulostore.broker.messages;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;

/**
 * @author Piotr Malicki
 */
public class WriteFinishedMessage extends Message {

  private static final long serialVersionUID = 6898301162202625940L;
  private final String writeModuleJobId_;
  private final ObjectId objectId_;

  public WriteFinishedMessage(String writeModuleJobId, ObjectId objectId) {
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
