package org.nebulostore.broker.messages;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.broker.Contract;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.errorresponder.ErrorResponder;
import org.nebulostore.communication.routing.errorresponder.SendAsyncMessageErrorResponder;

/**
 * Message send by another peer to acknowledge contract break.
 * @author szymon
 *
 */
public class BreakContractMessage extends CommMessage {

  private static final long serialVersionUID = -7437544862348721861L;

  private final Contract contract_;

  public BreakContractMessage(CommAddress sourceAddress, CommAddress destAddress,
      Contract contract) {
    super(sourceAddress, destAddress);
    contract_ = contract;
  }

  public Contract getContract() {
    return contract_;
  }

  @Override
  public ErrorResponder generateErrorResponder(BlockingQueue<Message> dispatcherQueue) {
    return new SendAsyncMessageErrorResponder(
        new AsyncBreakContractMessage(getSourceAddress(), contract_),
        getDestinationAddress(), dispatcherQueue);
  }

  @Override
  public boolean requiresAck() {
    return true;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }

}
