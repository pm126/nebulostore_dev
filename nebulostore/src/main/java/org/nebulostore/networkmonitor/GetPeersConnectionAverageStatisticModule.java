package org.nebulostore.networkmonitor;

import com.google.common.base.Predicate;
import com.google.inject.Inject;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Retrives from DHT the average of a given connectionAttribute for peer.
 *
 * @author szymonmatejczyk
 */
public class GetPeersConnectionAverageStatisticModule extends ReturningJobModule<Double> {
  private static Logger logger_ = Logger.getLogger(GetPeersConnectionAverageStatisticModule.class);
  /**
   * Peer we retrieve statistics of.
   */
  private final CommAddress peer_;


  private final Predicate<PeerConnectionSurvey> predicate_;

  public GetPeersConnectionAverageStatisticModule(CommAddress peer,
      Predicate<PeerConnectionSurvey> filter) {
    peer_ = peer;
    predicate_ = filter;
  }

  private static final String CONFIGURATION_PREFIX = "networkmonitor.";
  private XMLConfiguration config_;

  @Inject
  private void setConfig(XMLConfiguration config) {
    config_ = config;
  }

  private final MessageVisitor visitor_ = new GetStatisticsModuleVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  protected class GetStatisticsModuleVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) {
      RetrievePeersStatistics allStatsModule = new RetrievePeersStatistics(peer_, outQueue_);
      try {
        StatisticsList statsList = allStatsModule.getResult(config_
            .getInt(CONFIGURATION_PREFIX + "get-stats-timeout-secs"));
        endWithSuccess(statsList.calcWeightedMean(predicate_, true));
      } catch (NebuloException exception) {
        endWithError(exception);
      }
    }
  }

}
