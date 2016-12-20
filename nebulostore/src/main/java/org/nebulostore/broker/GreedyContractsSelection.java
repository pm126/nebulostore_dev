package org.nebulostore.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;

/**
 * Contract selection algorithm that is a hill-climbing(greedy) algorithm.
 *
 * @author szymon
 */
public class GreedyContractsSelection implements ContractsSelectionAlgorithm {

  private static Logger logger_ = Logger.getLogger(GreedyContractsSelection.class);

  private static final String CONFIGURATION_PREFIX = "broker.";

  private static Provider<ContractsEvaluator> contractsEvaluatorProvider_;
  private static long spaceContributedKb_;

  @Inject
  private void setDependencies(Provider<ContractsEvaluator> contractsEvaluatorProvider,
      @Named(CONFIGURATION_PREFIX + "space-contributed-kb") long spaceContributedKb) {
    contractsEvaluatorProvider_ = contractsEvaluatorProvider;
    spaceContributedKb_ = spaceContributedKb;
  }

  /**
   * Calculates contractsEvaluator value of all contracts but contract.
   */
  private double withoutContractValue(Contract contract, ContractsSet contracts,
      ContractsEvaluator contractsEvaluator) {
    double value = contractsEvaluator.evaluate(contracts, Predicates.<Contract>equalTo(contract));
    return value;
  }

  /**
   * If there exist a contract that is strictly worse than candidate returns one of the worst. Else
   * returns candidate.
   */
  private Contract findWorstWithCandidate(ContractsSet contracts, Contract candidate,
      ContractsEvaluator contractsEvaluator) {
    logger_.debug("Searching for the worst contract from contracts set: " + contracts +
        " with candidate: " + candidate);
    Iterator<Contract> it = contracts.iterator();

    double allValue = contractsEvaluator.evaluate(contracts, Predicates.<Contract>alwaysFalse());
    logger_.debug("allValue: " + allValue);
    double worstValue = allValue - withoutContractValue(candidate, contracts, contractsEvaluator);
    logger_.debug("initial worstValue: " + worstValue);
    Contract worstContract = candidate;
    while (it.hasNext()) {
      Contract contract = it.next();
      logger_.debug("Trying next contract: " + contract);
      double marginalValue = allValue -
          withoutContractValue(contract, contracts, contractsEvaluator);
      logger_.debug("Marginal value: " + marginalValue);
      if (marginalValue < worstValue) {
        worstValue = marginalValue;
        worstContract = contract;
      }
    }
    logger_.debug("Worst contract: " + worstContract);
    return worstContract;
  }

  /**
   * If accepting this contract and breaking previous to meet space constraints increases valuation,
   * returns true and a list of contracts that need to be broken. Else returns false and null.
   */
  @Override
  public OfferResponse responseToOffer(Contract newContract, ContractsSet currentContracts) {
    currentContracts.add(newContract);
    Set<Contract> toBreak = new HashSet<Contract>();
    ContractsEvaluator contractsEvaluator = contractsEvaluatorProvider_.get();
    while (currentContracts.realSize() > spaceContributedKb_) {
      Contract worst = findWorstWithCandidate(currentContracts, newContract, contractsEvaluator);
      if (worst == newContract) {
        currentContracts.addAll(toBreak);
        return new OfferResponse(false, null);
      } else {
        toBreak.add(worst);
        currentContracts.remove(worst);
      }
    }

    return new OfferResponse(true, toBreak);
  }

  /**
   * Returns the contract that increases valuation the most.
   */
  @Override
  public Contract chooseContractToOffer(Set<Contract> possibleContracts,
      ContractsSet currentContracts) {
    List<Contract> shuffledContractList = new ArrayList<Contract>(possibleContracts);
    Collections.shuffle(shuffledContractList);

    Iterator<Contract> it = shuffledContractList.iterator();
    double bestValue = -Double.MAX_VALUE;
    Contract bestContract = null;

    ContractsEvaluator contractsEvaluator = contractsEvaluatorProvider_.get();
    while (it.hasNext()) {
      Contract contract = it.next();
      logger_.debug("Evaluating next contract: " + contract);
      double value = contractsEvaluator.evaluate(currentContracts, new ContractsSet(contract));
      logger_.debug("Evaluation value: " + value);
      if (value > bestValue) {
        bestValue = value;
        bestContract = contract;
      }
    }

    return bestContract;
  }

  @Override
  public ImproveGroupResponse improveGroupOfContracts(ContractsSet group, ContractsSet candidates) {
    logger_.debug("Trying to improve group: " + group + " with candidates: " + candidates);
    ContractsEvaluator contractsEvaluator = contractsEvaluatorProvider_.get();
    double bestScore = contractsEvaluator.evaluate(group);
    logger_.debug("Current score: " + bestScore);
    Contract worstContract = null;
    Contract bestCandidate = null;
    for (Contract candidate : candidates) {
      logger_.debug("Checking next candidate" + candidate);
      group.add(candidate);
      Contract worst = findWorstWithCandidate(group, candidate, contractsEvaluator);
      logger_.debug("Found worst contract with this candidate: " + worst);
      if (!worst.equals(candidate)) {
        group.remove(worst);
        double score = contractsEvaluator.evaluate(group);
        logger_.debug("New score: " + score);
        group.add(worst);
        if (score > bestScore) {
          bestScore = score;
          worstContract = worst;
          bestCandidate = candidate;
        }
      }
      group.remove(candidate);
    }

    logger_.debug("Best candidate: " + bestCandidate + ", worst contract: " + worstContract);
    return new ImproveGroupResponse(bestCandidate, worstContract);
  }

}
