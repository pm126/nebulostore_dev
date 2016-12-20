package org.nebulostore.replicator.repairer.messages;

import org.nebulostore.communication.naming.CommAddress;

public class RepairSuccessfulMessage extends RepairResultMessage {

  private static final long serialVersionUID = 7915204478657360474L;

  public RepairSuccessfulMessage(String repairerJobId, CommAddress peer) {
    super(repairerJobId, peer);
  }
}
