package org.nebulostore.broker;

import com.google.common.base.Predicate;

/**
 * Contracts evaluator that counts only contract size.
 * @author szymon
 *
 */
public class OnlySizeContractsEvaluator extends ContractsEvaluatorWithDefault {
  /* Naive - only space sum */
  @Override
  public double evaluate(ContractsSet contracts, Predicate<Contract> predicate,
      ContractsSet added) {
    ContractsSet filtered = new ContractsSet();
    for (Contract c : contracts) {
      if (predicate.apply(c) || !c.isAvailable()) {
        filtered.add(c);
      }
    }
    return (contracts.realSize() - filtered.realSize()) + added.realSize();
  }
}
