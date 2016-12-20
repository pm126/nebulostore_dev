package org.nebulostore.peers;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.RegisterInstanceInDHTModule;
import org.nebulostore.appcore.RegisterKeyInDHTModule;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.AsyncMessagingModule;
import org.nebulostore.async.checker.MessageReceivingCheckerModule;
import org.nebulostore.async.synchrogroup.SynchroPeerSetChangeSequencerModule;
import org.nebulostore.broker.Broker;
import org.nebulostore.communication.CommunicationPeerFactory;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.EncryptionAPI.KeyType;
import org.nebulostore.crypto.keys.FileKeySource;
import org.nebulostore.crypto.session.InitSessionNegotiatorModule;
import org.nebulostore.dispatcher.Dispatcher;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.rest.RestModule;
import org.nebulostore.rest.RestModuleImpl;
import org.nebulostore.timer.Timer;

/**
 * This is a regular peer with full functionality. It creates, connects and runs all modules.
 * To create a different peer, subclass Peer and set its class name in configuration.
 *
 * To customize the Peer, please override initializeModules(), runActively() and cleanModules().
 *
 * @author Bolek Kulbabinski
 */
public class Peer extends AbstractPeer {
  private static Logger logger_ = Logger.getLogger(Peer.class);

  protected Thread dispatcherThread_;
  protected Thread networkThread_;
  protected AsyncMessagingModule asyncMessagingModule_;
  protected AsyncMessagesContext asyncMessagesContext_;
  protected SynchroPeerSetChangeSequencerModule synchroUpdateSequencer_;
  protected MessageReceivingCheckerModule msgReceivingChecker_;
  protected Thread msgReceivingCheckerThread_;

  protected BlockingQueue<Message> dispatcherInQueue_;
  protected BlockingQueue<Message> networkInQueue_;
  protected BlockingQueue<Message> commPeerInQueue_;
  protected BlockingQueue<Message> commPeerOutQueue_;

  protected AppKey appKey_;
  protected Broker broker_;
  protected Injector injector_;
  protected CommAddress commAddress_;
  protected Timer peerTimer_;
  protected NetworkMonitor networkMonitor_;
  protected InitSessionNegotiatorModule initSessionNegotiator_;

  private CommunicationPeerFactory commPeerFactory_;

  private int registrationTimeout_;

  private Thread restThread_;
  private boolean isRestEnabled_;
  private RestModule restModule_;

  private EncryptionAPI encryption_;
  private String publicKeyFilePath_;
  private String privateKeyFilePath_;
  private String publicKeyPeerId_;
  private String privateKeyPeerId_;

  @Inject
  public void setDependencies(@Named("DispatcherQueue") BlockingQueue<Message> dispatcherInQueue,
                              @Named("NetworkQueue") BlockingQueue<Message> networkQueue,
                              @Named("CommunicationPeerInQueue")
                                BlockingQueue<Message> commPeerInQueue,
                              @Named("CommunicationPeerOutQueue")
                                BlockingQueue<Message> commPeerOutQueue,
                              Broker broker,
                              AppKey appKey,
                              CommAddress commAddress,
                              CommunicationPeerFactory commPeerFactory,
                              Timer timer,
                              NetworkMonitor networkMonitor,
                              InitSessionNegotiatorModule initSessionNegotiator,
                              Injector injector,
                              @Named("peer.registration-timeout") int registrationTimeout,
                              AsyncMessagingModule asyncMessagingModule,
                              AsyncMessagesContext asyncMessagesContext,
                              SynchroPeerSetChangeSequencerModule synchroUpdateSequencer,
                              MessageReceivingCheckerModule msgReceivingChecker,
                              RestModuleImpl restModule,
                              @Named("rest-api.enabled") boolean isRestEnabled,
                              EncryptionAPI encryption,
                              @Named("security.public-key-file") String publicKeyFilePath,
                              @Named("security.private-key-file") String privateKeyFilePath,
                              @Named("PublicKeyPeerId") String publicKeyPeerId,
                              @Named("PrivateKeyPeerId") String privateKeyPeerId) {
    dispatcherInQueue_ = dispatcherInQueue;
    networkInQueue_ = networkQueue;
    commPeerInQueue_ = commPeerInQueue;
    commPeerOutQueue_ = commPeerOutQueue;
    broker_ = broker;
    appKey_ = appKey;
    commAddress_ = commAddress;
    commPeerFactory_ = commPeerFactory;
    peerTimer_ = timer;
    networkMonitor_ = networkMonitor;
    initSessionNegotiator_ = initSessionNegotiator;
    injector_ = injector;
    registrationTimeout_ = registrationTimeout;
    asyncMessagingModule_ = asyncMessagingModule;
    asyncMessagesContext_ = asyncMessagesContext;
    synchroUpdateSequencer_ = synchroUpdateSequencer;
    msgReceivingChecker_ = msgReceivingChecker;
    isRestEnabled_ = isRestEnabled;
    encryption_ = encryption;
    publicKeyFilePath_ = publicKeyFilePath;
    privateKeyFilePath_ = privateKeyFilePath;
    publicKeyPeerId_ = publicKeyPeerId;
    privateKeyPeerId_ = privateKeyPeerId;

    // Create core threads.
    Runnable dispatcher = new Dispatcher(dispatcherInQueue_, networkInQueue_, injector_);
    dispatcherThread_ = new Thread(dispatcher, "Dispatcher");
    Runnable commPeer = commPeerFactory_.newCommunicationPeer(commPeerInQueue_, commPeerOutQueue_);
    networkThread_ = new Thread(commPeer, "CommunicationPeer");
    if (isRestEnabled_) {
      restModule_ = restModule;
      restThread_ = new Thread(restModule_, "Rest Thread");
    }
    msgReceivingCheckerThread_ = new Thread(msgReceivingChecker_, "Messages receiving checker");
  }

  @Override
  public void quitNebuloStore() {
    logger_.info("Started quitNebuloStore().");
    if (msgReceivingChecker_ != null) {
      msgReceivingChecker_.getInQueue().add(new EndModuleMessage());
    }
    if (asyncMessagingModule_ != null) {
      asyncMessagingModule_.getInQueue().add(new EndModuleMessage());
    }
    if (broker_ != null) {
      broker_.getInQueue().add(new EndModuleMessage());
    }
    if (networkMonitor_ != null) {
      networkMonitor_.getInQueue().add(new EndModuleMessage());
    }
    commPeerInQueue_.add(new EndModuleMessage());
    if (dispatcherInQueue_ != null) {
      dispatcherInQueue_.add(new EndModuleMessage());
    }
    if (isRestEnabled_) {
      restModule_.shutDown();
    }
    logger_.info("Finished quitNebuloStore().");
  }

  @Override
  public final void run() {
    runPeer();
  }

  private void runPeer() {
    logger_.info("Running peer");
    initializeModules();
    logger_.info("Initialized modules");
    startCoreThreads();
    logger_.info("Started core threads");
    runActively();
    joinCoreThreads();
    cleanModules();
  }

  /**
   * Puts replication group under appKey_ in DHT and InstanceMetadata under commAddress_.
   * Register peer public and private keys.
   *
   * @param appKey
   */
  protected void register(AppKey appKey) {
    // TODO(bolek): This should be part of broker. (szm): or NetworkMonitor
    RegisterKeyInDHTModule registerKeyInDHTModule = new RegisterKeyInDHTModule(dispatcherInQueue_,
        appKey_);
    registerKeyInDHTModule.runThroughDispatcher();
    RegisterInstanceInDHTModule registerInstanceMetadataModule = new RegisterInstanceInDHTModule();
    registerInstanceMetadataModule.setDispatcherQueue(dispatcherInQueue_);
    registerInstanceMetadataModule.runThroughDispatcher();
    Metadata metadata = null;
    try {
      metadata = registerKeyInDHTModule.getResult(registrationTimeout_);
    } catch (NebuloException exception) {
      logger_.error("Unable to execute PutKeyModule!", exception);
    }
    if (metadata != null) {
      broker_.initialize(metadata);
    }

    InstanceMetadata instanceMetadata = null;
    try {
      instanceMetadata = registerInstanceMetadataModule.getResult(registrationTimeout_);
    } catch (NebuloException exception) {
      logger_.error("Unable to register InstanceMetadata!", exception);
    }
    if (instanceMetadata != null) {
      asyncMessagesContext_.initialize(instanceMetadata.getSynchroGroup(),
          instanceMetadata.getRecipients(),
          instanceMetadata.getRecipientsSetVersion(),
          instanceMetadata.getSynchroPeerCounters());
    } else {
      asyncMessagesContext_.initialize();
    }

    registerPeerPublicPrivateKeys();
  }

  /**
   * Initialize all optional modules and schedule them for execution by dispatcher.
   * Override this method to run modules selectively.
   */
  protected void initializeModules() {
    logger_.info("Initializing modules");
    runInitSessionNegotiator();
    logger_.info("Init session negotiator run");
    runNetworkMonitor();
    logger_.info("Network monitor run");
    runBroker();
    logger_.info("Broker run");
    runAsyncMessaging();
    logger_.info("Async messaging run");
  }

  /**
   * Logic to be executed when the application is already running.
   * Override this method when operations on active application are necessary.
   */
  protected void runActively() {
    // TODO: Move register to separate module or at least make it non-blocking.
    register(appKey_);
  }

  /**
   * Actions performed on exit.
   * Override this method when special clean-up is required.
   */
  protected void cleanModules() {
    // Empty by default.
  }

  protected void registerPeerPublicPrivateKeys() {
    try {
      encryption_.load(publicKeyPeerId_, new FileKeySource(publicKeyFilePath_, KeyType.PUBLIC),
          EncryptionAPI.STORE_IN_DHT);
      encryption_.load(privateKeyPeerId_, new FileKeySource(privateKeyFilePath_, KeyType.PRIVATE),
          !EncryptionAPI.STORE_IN_DHT);
    } catch (CryptoException e) {
      throw new RuntimeException("Unable to load public/private keys", e);
    }
  }

  protected void runInitSessionNegotiator() {
    logger_.info("Running init session negotiator");
    initSessionNegotiator_.runThroughDispatcher();
    logger_.info("Init session negotiator scheduled");
  }

  protected void runNetworkMonitor() {
    logger_.info("Running network monitor");
    networkMonitor_.runThroughDispatcher();
    logger_.info("Network monitor scheduled");
  }

  protected void runBroker() {
    logger_.info("Running broker");
    broker_.runThroughDispatcher();
    logger_.info("Broker scheduled");
  }

  protected void runAsyncMessaging() {
    logger_.info("Running async messaging");
    synchroUpdateSequencer_.runThroughDispatcher();
    logger_.info("Update sequencer scheduled");
    msgReceivingCheckerThread_.start();
    logger_.info("Receiving checker thread started");
    asyncMessagingModule_.runThroughDispatcher();
    logger_.info("Async messaging module scheduled");
  }

  protected void startCoreThreads() {
    logger_.info("Starting core threads");
    networkThread_.start();
    logger_.info("Network thread started");
    dispatcherThread_.start();
    logger_.info("Dispatcher thread started");
    if (isRestEnabled_) {
      restThread_.start();
    }
    logger_.info("Rest thread started");
  }

  protected void joinCoreThreads() {
    // Wait for threads to finish execution.
    try {
      msgReceivingCheckerThread_.join();
      networkThread_.join();
      dispatcherThread_.join();
      if (isRestEnabled_) {
        restThread_.join();
      }

    } catch (InterruptedException exception) {
      logger_.fatal("Interrupted");
      return;
    }
  }
}
