package org.nebulostore.systest.broker;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.broker.Contract;
import org.nebulostore.broker.ValuationBasedBroker;
import org.nebulostore.systest.broker.messages.BrokerContractsMessage;
import org.nebulostore.systest.broker.messages.GetBrokerContractsMessage;

/**
 * Valuation based broker that allows to retrieve its contracts by sending GetBrokerDataMessage.
 *
 * @author szymonmatejczyk
 *
 */
public class ValuationBasedBrokerWithContractsOpened extends ValuationBasedBroker {
  private static Logger logger_ = Logger.getLogger(ValuationBasedBrokerWithContractsOpened.class);

  public ValuationBasedBrokerWithContractsOpened() {
    visitor_ = new ThisVisitor();
  }

  /**
   * Visitor.
   */
  public class ThisVisitor extends ValuationBrokerVisitor {
    public void visit(GetBrokerContractsMessage message) {
      logger_.debug("Got GetBrokerContractsMessage.");
      Set<Contract> allContracts = new HashSet<>(allContracts_);
      outQueue_.add(new BrokerContractsMessage(message.getId(), allContracts));
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
