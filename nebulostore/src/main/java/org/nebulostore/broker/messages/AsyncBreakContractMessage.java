package org.nebulostore.broker.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.broker.Contract;
import org.nebulostore.communication.naming.CommAddress;

public class AsyncBreakContractMessage extends AsynchronousMessage {

  private static final long serialVersionUID = -1471975019886138840L;

  private final CommAddress senderAddress_;
  private final Contract contract_;

  public AsyncBreakContractMessage(CommAddress senderAddress, Contract contract) {
    senderAddress_ = senderAddress;
    contract_ = contract;
  }

  public CommAddress getSenderAddress() {
    return senderAddress_;
  }

  public Contract getContract() {
    return contract_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }
}
