package org.nebulostore.broker;


import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.nebulostore.coding.AvailabilityAnalyzer;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.networkmonitor.StatisticsList;

public class AvailabilityBasedContractEvaluator extends ContractsEvaluatorWithDefault {

  private final AvailabilityAnalyzer availAnalyzer_;
  private final NetworkMonitor networkMonitor_;
  private final Map<CommAddress, StatisticsList> statisticsMap_;

  @Inject
  public AvailabilityBasedContractEvaluator(AvailabilityAnalyzer availAnalyzer,
      NetworkMonitor networkMonitor) {
    availAnalyzer_ = availAnalyzer;
    networkMonitor_ = networkMonitor;
    statisticsMap_ = networkMonitor_.getStatisticsMap();
  }

  @Override
  public double evaluate(ContractsSet contracts, Predicate<Contract> filter,
      ContractsSet additional) {
    ContractsSet filtered = new ContractsSet();
    for (Contract contract : contracts) {
      if (filter.apply(contract)) {
        filtered.add(contract);
      }
    }

    return availAnalyzer_.analyzeAvailability(new ContractsSet(Sets.union(Sets.difference(
        contracts, filtered), additional)), statisticsMap_);
  }

}
