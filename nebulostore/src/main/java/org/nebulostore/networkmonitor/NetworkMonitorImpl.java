package org.nebulostore.networkmonitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.keys.DHTKeySource;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.messages.ConnectionTestMessage;
import org.nebulostore.networkmonitor.messages.ConnectionTestResponseMessage;
import org.nebulostore.timer.MessageGenerator;
import org.nebulostore.timer.Timer;

/**
 * Basic implementation of NetworkMonitor.
 *
 * @author szymon
 *
 */
public class NetworkMonitorImpl extends NetworkMonitor {
  private final Logger logger_ = Logger.getLogger(NetworkMonitor.class);
  private final Timer timer_;

  /**
   * Providers for NetworkMonitor submodules.
   */
  private final Provider<RandomPeersGossipingModule> randomPeersGossipingModuleProvider_;

  protected static final String CONFIGURATION_PREFIX = "networkmonitor.";

  private final Set<CommAddress> knownPeers_ = new HashSet<CommAddress>();
  private final List<CommAddress> knownPeersVector_ = new Vector<CommAddress>();

  private Set<CommAddress> randomPeersSample_ = Collections
      .synchronizedSet(new HashSet<CommAddress>());
  private final CommAddress commAddress_;

  protected BlockingQueue<Message> dispatcherQueue_;

  protected Provider<ConnectionTestMessageHandler> connectionTestMessageHandlerProvider_;

  private final long statisticsUpdateIntervalMillis_;

  protected MessageVisitor visitor_;

  private final EncryptionAPI encryptionAPI_;

  private final Map<CommAddress, String> peersPublicKeyId_;
  private final Map<CommAddress, StatisticsList> statisticsMap_ = new ConcurrentHashMap<>();

  @Inject
  public NetworkMonitorImpl(@Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      CommAddress commAddress,
      Timer timer,
      Provider<RandomPeersGossipingModule> randomPeersGossipingModuleProvider,
      Provider<ConnectionTestMessageHandler> connectionTestMessageHandlerProvider,
      @Named(CONFIGURATION_PREFIX + "statistics-update-interval-millis")
      long statisticsUpdateIntervalMillis,
      EncryptionAPI encryptionAPI) {
    dispatcherQueue_ = dispatcherQueue;
    commAddress_ = commAddress;
    knownPeers_.add(commAddress_);
    knownPeersVector_.add(commAddress_);
    timer_ = timer;
    randomPeersGossipingModuleProvider_ = randomPeersGossipingModuleProvider;
    connectionTestMessageHandlerProvider_ = connectionTestMessageHandlerProvider;
    statisticsUpdateIntervalMillis_ = statisticsUpdateIntervalMillis;
    encryptionAPI_ = encryptionAPI;
    peersPublicKeyId_ = new HashMap<CommAddress, String>();
    visitor_ = new NetworkMonitorVisitor();
  }

  /**
   * Messages to be send to dispatcher when context changes.
   */
  private final Set<MessageGenerator> contextChangeMessageGenerators_ = Collections
      .newSetFromMap(new ConcurrentHashMap<MessageGenerator, Boolean>());

  private void contextChanged() {
    logger_.debug("context changed");
    for (MessageGenerator m : contextChangeMessageGenerators_) {
      logger_.debug("sending CC message");
      dispatcherQueue_.add(m.generate());
    }
  }

  @Override
  public void addContextChangeMessageGenerator(MessageGenerator generator) {
    logger_.debug("Adding contextChangeMessageGenerator.");
    contextChangeMessageGenerators_.add(generator);
  }

  @Override
  public void removeContextChangeMessageGenerator(MessageGenerator generator) {
    contextChangeMessageGenerators_.remove(generator);
  }

  @Override
  public synchronized List<CommAddress> getKnownPeers() {
    return new ArrayList<CommAddress>(knownPeersVector_);
  }

  @Override
  public synchronized void addFoundPeer(CommAddress address) {
    // TODO(mbw): address != null, because of Broker.java:40
    if (!knownPeers_.contains(address) && address != null) {
      logger_.info("Adding a CommAddress: " + address);
      knownPeers_.add(address);
      knownPeersVector_.add(address);
      if (randomPeersSample_.size() < RandomPeersGossipingModule.RANDOM_PEERS_SAMPLE_SIZE) {
        randomPeersSample_.add(address);
        logger_.info("Updating randomPeersSample, current version: " + randomPeersSample_);
      }
      contextChanged();
    }
  }

  @Override
  public synchronized Set<CommAddress> getRandomPeersSample() {
    List<CommAddress> knownPeers = new ArrayList<>(knownPeersVector_);
    Collections.shuffle(knownPeers);
    Set<CommAddress> randomPeersSample = new HashSet<>(knownPeers.subList(0,
        Math.min(RandomPeersGossipingModule.RANDOM_PEERS_SAMPLE_SIZE, knownPeers.size())));
    logger_.info("Selected new set: " + randomPeersSample);
    return randomPeersSample;
  }

  @Override
  public synchronized void setRandomPeersSample(Set<CommAddress> randomPeersSample) {
    logger_.debug("Set random peers sample size: " + randomPeersSample.size() + " was: " +
        randomPeersSample_.size());
    randomPeersSample_ = Collections.synchronizedSet(new HashSet<CommAddress>(randomPeersSample));
  }

  @Override
  public String getPeerPublicKeyId(CommAddress peer) {
    String result = peersPublicKeyId_.get(peer);
    if (result == null) {
      try {
        result = CryptoUtils.getRandomString();
        peersPublicKeyId_.put(peer, result);
        logger_.debug(String.format("Put (Peer address %s, KeyID %s)", peer, result));
        encryptionAPI_.load(result, new DHTKeySource(peer, dispatcherQueue_),
            !EncryptionAPI.STORE_IN_DHT);
      } catch (CryptoException e) {
        logger_.error("Unable to load peer public key", e);
      }
    }
    return result;
  }

  @Override
  public synchronized void putAllStatistics(Map<CommAddress, StatisticsList> statistics) {
    statisticsMap_.putAll(statistics);
  }

  @Override
  public synchronized Map<CommAddress, StatisticsList> getStatisticsMap() {
    return Maps.newHashMap(statisticsMap_);
  }

  public class NetworkMonitorVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Initialized...");
      timer_.scheduleRepeatedJob(randomPeersGossipingModuleProvider_,
          statisticsUpdateIntervalMillis_, statisticsUpdateIntervalMillis_);
    }

    public void visit(ConnectionTestMessage message) {
      logger_.debug("Got ConnectionTestMessage.");
      //ConnectionTestMessageHandler handler = connectionTestMessageHandlerProvider_.get();
      //message.setHandler(handler);
      //dispatcherQueue_.add(message);
      networkQueue_.add(new ConnectionTestResponseMessage(message.getId(), message
          .getSourceAddress()));
    }

    public void visit(EndModuleMessage message) {
      logger_.debug("Ending the network monitor");
      timer_.cancelTimer();
      endJobModule();
    }
  }

  @Override
  protected synchronized void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
