package org.nebulostore.systest.broker;

import java.util.LinkedList;
import java.util.List;

import org.nebulostore.broker.Contract;
import org.nebulostore.conductor.CaseStatistics;

/**
 * Statistics gathered by the test - list of peer's contracts.
 *
 * @author szymonmatejczyk
 */
public class BrokerTestStatistics extends CaseStatistics {
  private static final long serialVersionUID = -65101433372161217L;
  private final List<Contract> contracts_ = new LinkedList<>();

  public List<Contract> getContracts() {
    return contracts_;
  }

  public void addContract(Contract contract) {
    contracts_.add(contract);
  }
}
