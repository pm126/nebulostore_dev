package org.nebulostore.dfuntest.coding.broker;

import java.util.Random;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.broker.Contract;
import org.nebulostore.broker.ValuationBasedBroker;
import org.nebulostore.broker.messages.BreakContractMessage;
import org.nebulostore.dispatcher.JobInitMessage;

public class ContractsBreakingValuationBasedBroker extends ValuationBasedBroker {

  private static final int CONTRACT_BREAK_PERIOD_MILIS = 120000;

  public ContractsBreakingValuationBasedBroker() {
    visitor_ = new BreakingBrokerVisitor();
  }

  protected class BreakingBrokerVisitor extends ValuationBrokerVisitor {

    @Override
    public void visit(JobInitMessage message) {
      super.visit(message);
      timer_.scheduleRepeated(new BreakUnusedContractMessage(jobId_), CONTRACT_BREAK_PERIOD_MILIS,
          CONTRACT_BREAK_PERIOD_MILIS);
    }

    public void visit(BreakUnusedContractMessage message) {
//      logger_.debug("Unused contracts size: " + unusedContracts_.size());
//      if (!unusedContracts_.isEmpty()) {
//        int indexToBreak = new Random().nextInt(unusedContracts_.size());
//        for (Contract contract : unusedContracts_) {
//          if (indexToBreak == 0) {
//            //FIXME wiadomość async o zerwaniu kontraktu
//            logger_.debug("Breaking the contract: " + contract + " on the purpose of test");
//            networkQueue_.add(new BreakContractMessage(myAddress_, contract.getPeer(), contract));
//            removeContract(contract);
//            break;
//          }
//          indexToBreak--;
//        }
//
//      }
    }
  }

  private class BreakUnusedContractMessage extends Message {

    private static final long serialVersionUID = -8049761325505281790L;

    public BreakUnusedContractMessage(String jobId) {
      super(jobId);
    }

  }

}
