package org.nebulostore.coding;

import java.util.Map;

import org.nebulostore.broker.ContractsSet;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.StatisticsList;

/**
 * @author Piotr Malicki
 */
public interface AvailabilityAnalyzer {

  /**
   * Analyze availability of data in given contracts set when using specific erasure code based on
   * given statistics.
   */
  double analyzeAvailability(ContractsSet contracts, Map<CommAddress, StatisticsList> statistics);
}
