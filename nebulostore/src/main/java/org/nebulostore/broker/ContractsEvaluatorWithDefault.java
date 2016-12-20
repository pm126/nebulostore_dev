package org.nebulostore.broker;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Implements method using only the most general one.
 * @author szymon
 */
public abstract class ContractsEvaluatorWithDefault implements ContractsEvaluator {

  @Override
  public double evaluate(ContractsSet contracts) {
    return evaluate(contracts, Predicates.<Contract>alwaysFalse());
  }

  @Override
  public double evaluate(ContractsSet contracts, Predicate<Contract> filter) {
    return evaluate(contracts, filter, new ContractsSet());
  }

  @Override
  public double evaluate(ContractsSet contracts, ContractsSet additional) {
    return evaluate(contracts, Predicates.<Contract>alwaysFalse(), additional);
  }
}
