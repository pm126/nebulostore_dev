package org.nebulostore.peers;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Functions;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.api.DeleteNebuloObjectModule;
import org.nebulostore.api.GetNebuloObjectModule;
import org.nebulostore.api.WriteNebuloObjectModule;
import org.nebulostore.api.WriteNebuloObjectPartsModule;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.appcore.model.NebuloObjectFactoryImpl;
import org.nebulostore.appcore.model.ObjectDeleter;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.appcore.model.ObjectWriter;
import org.nebulostore.appcore.model.PartialObjectWriter;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.checker.MessageReceivingCheckerModule;
import org.nebulostore.async.synchrogroup.SynchroPeerSetChangeSequencerModule;
import org.nebulostore.async.synchrogroup.selector.LimitedPeerNumSynchroPeerSelector;
import org.nebulostore.async.synchrogroup.selector.SynchroPeerSelector;
import org.nebulostore.broker.AvailabilityBasedContractEvaluator;
import org.nebulostore.broker.Broker;
import org.nebulostore.broker.ContractsEvaluator;
import org.nebulostore.broker.ContractsSelectionAlgorithm;
import org.nebulostore.broker.GreedyContractsSelection;
import org.nebulostore.broker.ValuationBasedBroker;
import org.nebulostore.coding.AdditionBasedAvailabilityAnalyzer;
import org.nebulostore.coding.AvailabilityAnalyzer;
import org.nebulostore.coding.ObjectRecreationChecker;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.coding.pyramid.PyramidObjectRecreationChecker;
import org.nebulostore.coding.pyramid.PyramidObjectRecreator;
import org.nebulostore.coding.pyramid.PyramidReplicaPlacementPreparator;
import org.nebulostore.coding.repetition.RepetitionObjectRecreationChecker;
import org.nebulostore.coding.repetition.RepetitionObjectRecreator;
import org.nebulostore.coding.repetition.RepetitionReplicaPlacementPreparator;
import org.nebulostore.communication.CommunicationFacadeAdapterConfiguration;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.BasicEncryptionAPI;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.InitSessionContext;
import org.nebulostore.crypto.session.InitSessionNegotiatorModule;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.DefaultConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.networkmonitor.NetworkMonitorImpl;
import org.nebulostore.persistence.FileStore;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.ReplicatorImpl;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.repairer.ReplicaRepairerModuleFactory;
import org.nebulostore.rest.BrokerResource;
import org.nebulostore.rest.NetworkMonitorResource;
import org.nebulostore.rest.ReplicatorResource;
import org.nebulostore.rest.RestModule;
import org.nebulostore.rest.RestModuleImpl;
import org.nebulostore.subscription.api.SimpleSubscriptionNotificationHandler;
import org.nebulostore.subscription.api.SubscriptionNotificationHandler;
import org.nebulostore.timer.Timer;
import org.nebulostore.timer.TimerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration (all dependencies and constants) of a regular Nebulostore peer.
 *
 * @author Bolek Kulbabinski
 */
public class PeerConfiguration extends GenericConfiguration {

  private static Logger logger_ = LoggerFactory.getLogger(PeerConfiguration.class);

  private static final int ASYNC_MODULE_SYNC_THREAD_POOL_SIZE = 1;
  private static final int ASYNC_MODULE_CACHE_REFRESH_THREAD_POOL_SIZE = 1;

  private static final String REPETITION_CODING_TYPE = "repetition";
  private static final String PYRAMID_CODING_TYPE = "pyramid";

  @Override
  protected void configureAll() {
    logger_.info("Configuring all");
    bind(XMLConfiguration.class).toInstance(config_);

    AppKey appKey = new AppKey(config_.getString("app-key", ""));
    bind(AppKey.class).toInstance(appKey);
    bind(CommAddress.class).toInstance(
        new CommAddress(config_.getString("communication.comm-address", "")));
    configureQueues();
    logger_.info("Queues bound");

    configureEncryption();
    logger_.info("Encryption configured");

    configureObjectGetters();
    logger_.info("Object getters configured");
    configureObjectWriters();
    logger_.info("Object writers configured");
    configureObjectDeleters();
    logger_.info("Object deleters configured");

    bind(NebuloObjectFactory.class).to(NebuloObjectFactoryImpl.class);
    bind(SubscriptionNotificationHandler.class).to(SimpleSubscriptionNotificationHandler.class);
    bind(Timer.class).to(TimerImpl.class);

    configureAdditional();
    logger_.info("Additional configured");
    configureBroker();
    logger_.info("Broker configured");
    configureCommunicationPeer();
    logger_.info("Communication configured");
    configureNetworkMonitor();
    logger_.info("Network monitor configured");
    configureAsyncMessaging();
    logger_.info("Async Messaging configured");
    configurePeer();
    logger_.info("Peer configured");
    configureReplicator(appKey);
    logger_.info("Replicator configured");
    configureRestModule();
    logger_.info("Rest configured");
    configureSessionNegotiator();
    logger_.info("Session negotiator configured");
    configureErasureCoding();
    logger_.info("Erasure coding configured");
  }

  protected void configureEncryption() {
    bind(EncryptionAPI.class).to(BasicEncryptionAPI.class).in(Scopes.SINGLETON);
    bind(String.class).annotatedWith(
        Names.named("PublicKeyPeerId")).toInstance(CryptoUtils.getRandomString());
    bind(String.class).annotatedWith(
        Names.named("PrivateKeyPeerId")).toInstance(CryptoUtils.getRandomString());
  }

  private void configureSessionNegotiator() {
    bind(InitSessionContext.class).in(Scopes.SINGLETON);
    bind(InitSessionNegotiatorModule.class).in(Scopes.SINGLETON);
  }

  private void configureReplicator(AppKey appKey) {
    KeyValueStore<byte[]> replicatorStore;
    try {
      String pathPrefix = config_.getString("replicator.storage-path") + "/" +
          appKey.getKey().toString() + "_storage/";
      replicatorStore = new FileStore<byte[]>(pathPrefix,
        Functions.<byte[]>identity(), Functions.<byte[]>identity());
    } catch (IOException e) {
      throw new RuntimeException("Unable to configure Replicator module", e);
    }
    bind(new TypeLiteral<KeyValueStore<byte[]>>() { }).
      annotatedWith(Names.named("ReplicatorStore")).toInstance(replicatorStore);
    bind(Replicator.class).to(ReplicatorImpl.class);
  }

  protected void configureAdditional() {
  }

  protected void configurePeer() {
    bind(AbstractPeer.class).to(Peer.class).in(Scopes.SINGLETON);
  }

  protected void configureCommunicationPeer() {
    AbstractModule absModule;
    absModule = new CommunicationFacadeAdapterConfiguration(config_);
    install(absModule);
  }

  protected void configureBroker() {
    bind(Broker.class).to(ValuationBasedBroker.class).in(Scopes.SINGLETON);
    bind(ContractsSelectionAlgorithm.class).to(GreedyContractsSelection.class);
    bind(ContractsEvaluator.class).to(AvailabilityBasedContractEvaluator.class);
  }

  protected void configureNetworkMonitor() {
    bind(NetworkMonitor.class).to(NetworkMonitorImpl.class).in(Scopes.SINGLETON);
    bind(ConnectionTestMessageHandler.class).to(DefaultConnectionTestMessageHandler.class);
  }

  protected void configureObjectGetters() {
    bind(ObjectGetter.class).to(GetNebuloObjectModule.class);
  }

  protected void configureObjectWriters() {
    bind(ObjectWriter.class).to(WriteNebuloObjectModule.class);
    bind(PartialObjectWriter.class).to(WriteNebuloObjectPartsModule.class);
  }

  protected void configureObjectDeleters() {
    bind(ObjectDeleter.class).to(DeleteNebuloObjectModule.class);
  }

  protected void configureAsyncMessaging() {
    bind(AsyncMessagesContext.class).in(Scopes.SINGLETON);
    bind(ScheduledExecutorService.class).annotatedWith(
        Names.named("async.sync-executor")).toInstance(
        Executors.newScheduledThreadPool(ASYNC_MODULE_SYNC_THREAD_POOL_SIZE));
    bind(ScheduledExecutorService.class).annotatedWith(
        Names.named("async.cache-refresh-executor")).toInstance(
        Executors.newScheduledThreadPool(ASYNC_MODULE_CACHE_REFRESH_THREAD_POOL_SIZE));
    bind(SynchroPeerSetChangeSequencerModule.class).in(Scopes.SINGLETON);
    bind(MessageReceivingCheckerModule.class).in(Scopes.SINGLETON);
    configureAsyncSelector();
  }

  protected void configureAsyncSelector() {
    bind(SynchroPeerSelector.class).to(LimitedPeerNumSynchroPeerSelector.class).
      in(Scopes.SINGLETON);
  }

  protected void configureQueues() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> dispatcherQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> commPeerInQueue = new LinkedBlockingQueue<Message>();

    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("NetworkQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommunicationPeerInQueue")).toInstance(commPeerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("MsgReceivingCheckerNetworkQueue")).toInstance(commPeerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("MsgReceivingCheckerInQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("MsgReceivingCheckerOutQueue")).toInstance(dispatcherQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("DispatcherQueue")).toInstance(dispatcherQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommunicationPeerOutQueue")).toInstance(networkQueue);
  }

  protected void configureRestModule() {
    bind(BrokerResource.class);
    bind(NetworkMonitorResource.class);
    bind(ReplicatorResource.class);
    bind(RestModule.class).to(RestModuleImpl.class);
  }

  protected void configureErasureCoding() {
    switch (config_.getString("coding.type")) {
      case PYRAMID_CODING_TYPE:
        bind(ReplicaPlacementPreparator.class).to(PyramidReplicaPlacementPreparator.class);
        bind(ObjectRecreator.class).to(PyramidObjectRecreator.class);
        bind(ObjectRecreationChecker.class).to(PyramidObjectRecreationChecker.class);
        break;
      case REPETITION_CODING_TYPE:
      default:
        bind(ReplicaPlacementPreparator.class).to(RepetitionReplicaPlacementPreparator.class);
        bind(ObjectRecreator.class).to(RepetitionObjectRecreator.class);
        bind(ObjectRecreationChecker.class).to(RepetitionObjectRecreationChecker.class);
        break;
    }

    install(new FactoryModuleBuilder().build(ReplicaRepairerModuleFactory.class));
    bind(AvailabilityAnalyzer.class).to(AdditionBasedAvailabilityAnalyzer.class);
  }
}
