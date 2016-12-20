package org.nebulostore.broker;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Predicate;

/**
 * Method of evaluating contracts list.
 * @author szymonmatejczyk
 */
public interface ContractsEvaluator {

  /**
   * Indicates which contracts should be filtered.
   */
  public interface ContractFilter {
    boolean filter(Contract contract);
  }

  /**
   * Doesn't filter any contract.
   */
  public class None implements ContractFilter {
    @Override
    public boolean filter(Contract contract) {
      return false;
    }
  };

  /**
   * Filters contracts in filtered_ set.
   */
  public class WithoutSet implements ContractFilter {
    private final Set<Contract> filtered_;

    public WithoutSet(HashSet<Contract> filtered) {
      filtered_ = filtered;
    }

    @Override
    public boolean filter(Contract contract) {
      return filtered_.contains(contract);
    }
  }

  /**
   * Calculates valuation of contracts but the ones that are filtered by filter. We assume that
   * valuation v fulfills following conditions: 1. v(S) = 0 iff S = empty set 2. v(S) >= 0 for every
   * S != empty set.
   */
  double evaluate(ContractsSet contracts);

  double evaluate(ContractsSet contracts, Predicate<Contract> filter);

  double evaluate(ContractsSet contracts, Predicate<Contract> filter, ContractsSet additional);

  double evaluate(ContractsSet contracts, ContractsSet additional);
}
