package org.nebulostore.dfuntest.coding.broker;

import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.broker.Broker;
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.dispatcher.JobInitMessage;

public class AlwaysDenyingBroker extends Broker {

  public AlwaysDenyingBroker() {
    visitor_ = new DenyingBrokerVisitor();
  }

  protected class DenyingBrokerVisitor extends BrokerVisitor {

    public void visit(JobInitMessage message) {

    }

    public void visit(ContractOfferMessage message) {
      networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
          message.getEncryptedContract(), message.getSessionId(), false));
    }

    public void visit(EndModuleMessage message) {

    }

  }

}
