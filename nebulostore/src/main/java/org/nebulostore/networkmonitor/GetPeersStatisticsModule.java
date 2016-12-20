package org.nebulostore.networkmonitor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Sets;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * @author Piotr Malicki
 */
public class GetPeersStatisticsModule extends
    ReturningJobModule<Map<CommAddress, StatisticsList>> {

  private static Logger logger_ = Logger.getLogger(GetPeersStatisticsModule.class);

  private final Map<CommAddress, StatisticsList> statisticsMap_ = new HashMap<>();
  private final Set<CommAddress> peers_;
  private final Map<KeyDHT, CommAddress> waitingForResults_ = new HashMap<>();
  private final MessageVisitor visitor_ = new GetStatisticsMessageVisitor();

  public GetPeersStatisticsModule(Set<CommAddress> peers, BlockingQueue<Message> dispatcherQueue) {
    outQueue_ = dispatcherQueue;
    peers_ = Sets.newHashSet(peers);
  }

  protected class GetStatisticsMessageVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Starting " + GetPeersStatisticsModule.class.getSimpleName() + " with " +
          "addresses: " + peers_);
      for (CommAddress replicator : peers_) {
        networkQueue_.add(new GetDHTMessage(jobId_, replicator.toKeyDHT()));
        waitingForResults_.put(replicator.toKeyDHT(), replicator);
      }
    }

    public void visit(ValueDHTMessage message) {
      if (message.getValue().getValue() instanceof InstanceMetadata) {
        InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
        CommAddress peer = waitingForResults_.remove(message.getKey());
        logger_.debug("Received statistics for peer " + peer);
        logger_.debug("Still waiting for " + waitingForResults_.size() + " peers.");
        if (peer != null) {
          statisticsMap_.put(peer, metadata.getStatistics());
          tryEndModule();
        } else {
          logIncorrectMessage(message);
        }
      } else {
        logIncorrectMessage(message);
      }
    }

    public void visit(ErrorDHTMessage message) {
      if (message.getRequestMessage() instanceof GetDHTMessage) {
        GetDHTMessage getMessage = (GetDHTMessage) message.getRequestMessage();
        logger_.warn("Error in retrieving statistics using key: " + getMessage.getKey());
        CommAddress replicator = waitingForResults_.remove(getMessage.getKey());
        if (replicator != null) {
          tryEndModule();
        } else {
          logIncorrectMessage(message);
        }
      } else {
        logIncorrectMessage(message);
      }
    }

    private void tryEndModule() {
      logger_.debug("Trying to end module. Still waiting for " + waitingForResults_.size() +
          " statistics lists");
      if (waitingForResults_.isEmpty()) {
        endWithSuccess(statisticsMap_);
      }
    }

    private void logIncorrectMessage(Message message) {
      logger_.warn("Received unexpected" + message.getClass().getSimpleName() + " .");
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
