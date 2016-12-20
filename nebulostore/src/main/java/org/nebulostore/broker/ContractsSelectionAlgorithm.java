package org.nebulostore.broker;

import java.util.Set;

/**
 * Calculates response for contract offer and decides which offer to send.
 * @author szymon
 */
public interface ContractsSelectionAlgorithm {
  /** Response for contract offer. */
  class OfferResponse {
    public boolean responseAnswer_;
    public Set<Contract> contractsToBreak_;

    public OfferResponse(boolean responseAnswer, Set<Contract> contractsToBreak) {
      responseAnswer_ = responseAnswer;
      contractsToBreak_ = contractsToBreak;
    }
  }

  class ImproveGroupResponse {
    public Contract contractToAdd_;
    public Contract contractToBreak_;

    public ImproveGroupResponse(Contract contractToAdd, Contract contractToBreak) {
      contractToAdd_ = contractToAdd;
      contractToBreak_ = contractToBreak;
    }
  }

  OfferResponse responseToOffer(Contract newContract, ContractsSet currentContracts);

  Contract chooseContractToOffer(Set<Contract> possibleContracts,
      ContractsSet currentContracts);

  /**
   * Propose changes to the group of contracts. The number of contracts to break and the number
   * of contracts to add are guaranteed to be equal.
   *
   * @param group current contracts in the given group
   * @param candidates candidates to add to the group
   * @return
   */
  ImproveGroupResponse improveGroupOfContracts(ContractsSet group, ContractsSet candidates);
}
