package org.nebulostore.coding;

import java.util.Map;

import org.apache.log4j.Logger;
import org.nebulostore.broker.Contract;
import org.nebulostore.broker.ContractsSet;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.ConnectionAttribute;
import org.nebulostore.networkmonitor.StatisticsList;

/**
 * @author Piotr Malicki
 */
public class AdditionBasedAvailabilityAnalyzer implements AvailabilityAnalyzer {

  private static Logger logger_ = Logger.getLogger(AdditionBasedAvailabilityAnalyzer.class);

  @Override
  public double analyzeAvailability(ContractsSet contracts,
      Map<CommAddress, StatisticsList> statistics) {
    /*logger_.debug("Analyzing availability of contracts set: " + contracts + " with statistics :
     *  " + statistics);*/
    double availabilitySum = 0.0;
    for (Contract contract : contracts) {
      //logger_.debug("Checking next contract: " + contract);
      CommAddress replicator = contract.getPeer();
      if (contract.isAvailable()) {
        StatisticsList statisticsList = statistics.get(replicator);
        //logger_.debug("Statistics list: " + statisticsList);
        if (statisticsList != null) {
          /*logger_.warn("No statistics available for peer " + replicator);
        } else {*/
          /*logger_.debug("Weighted mean: " +
              statisticsList.calcWeightedMean(ConnectionAttribute.AVAILABILITY));*/
          availabilitySum += statisticsList.calcWeightedMean(ConnectionAttribute.AVAILABILITY);
          logger_.debug("Current availability sum: " + availabilitySum);
        }
      }
    }

    return availabilitySum;
  }

}
