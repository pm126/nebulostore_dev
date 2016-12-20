package org.nebulostore.replicator.repairer.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.communication.naming.CommAddress;

public class RepairResultMessage extends Message {

  private static final long serialVersionUID = 683805866994000424L;

  private final String repairerJobId_;
  private final CommAddress peer_;

  public RepairResultMessage(String repairerJobId, CommAddress peer) {
    repairerJobId_ = repairerJobId;
    peer_ = peer;
  }

  public String getRepairerJobId() {
    return repairerJobId_;
  }

  public CommAddress getPeer() {
    return peer_;
  }

  @Override
  public JobModule getHandler() {
    return new BrokerMessageForwarder(this);
  }
}
