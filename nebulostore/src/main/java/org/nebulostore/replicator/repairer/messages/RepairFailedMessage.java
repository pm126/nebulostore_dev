package org.nebulostore.replicator.repairer.messages;

import org.nebulostore.communication.naming.CommAddress;

public class RepairFailedMessage extends RepairResultMessage {

  private static final long serialVersionUID = -2331252665158558357L;

  private final Exception exception_;

  public RepairFailedMessage(String repairerJobId, Exception exception, CommAddress peer) {
    super(repairerJobId, peer);
    exception_ = exception;
  }

  public Exception getException() {
    return exception_;
  }


}
