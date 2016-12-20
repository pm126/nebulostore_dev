package org.nebulostore.async;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.synchrogroup.CacheRefreshingModule;
import org.nebulostore.async.synchrogroup.SynchroPeerSetChangeSequencerModule;
import org.nebulostore.async.synchrogroup.messages.LastFoundPeerMessage;
import org.nebulostore.async.synchronization.SynchronizeAsynchronousMessagesModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;

/**
 * Class responsible for all functions of asynchronous messages sending. It involves:<br>
 * - initializing async messaging context<br>
 * - registering selection modules after discovery of a new peer<br>
 * - running synchronization module periodically<br>
 * - running cache refresh module periodically<br>
 * <br>
 * This modules ensures that:<br>
 * - if any synchro-peer holding a message synchronizes with original recipient at some time,
 * this message will be delivered to the original recipient.<br>
 * <br>
 * IMPORTANT:<br>
 * There is no guarantee that every message sent asynchronously will be delivered exactly one
 * time. Each messsage may be delivered one or more times to the proper receiver. All modules
 * using this module to send messages asynchronously should take this into account.
 *
 * @author Piotr Malicki
 */

public class AsyncMessagingModule extends JobModule {

  private static final long SYNCHRONIZATION_PERIOD_SEC = 60;
  private static final long CACHE_REFRESH_PERIOD_SEC = 120;

  private static Logger logger_ = Logger.getLogger(AsyncMessagingModule.class);

  private final SynchronizationService syncService_;
  private final ScheduledExecutorService synchronizationExecutor_;
  private final ScheduledExecutorService cacheRefreshExecutor_;

  private final NetworkMonitor networkMonitor_;
  private final BlockingQueue<Message> dispatcherQueue_;
  private final AsyncMessagesContext context_;
  private final MessageVisitor visitor_ = new AsyncMessagingModuleVisitor();
  private final SynchroPeerSetChangeSequencerModule synchroSequencer_;

  private final CacheRefreshingService cacheRefreshingService_;

  @Inject
  public AsyncMessagingModule(
      @Named("async.sync-executor") final ScheduledExecutorService synchronizationExecutor,
      @Named("async.cache-refresh-executor") final ScheduledExecutorService cacheRefreshExecutor,
      final NetworkMonitor networkMonitor,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      AsyncMessagesContext context,
      SynchroPeerSetChangeSequencerModule synchroSequencer) {
    synchronizationExecutor_ = synchronizationExecutor;
    cacheRefreshExecutor_ = cacheRefreshExecutor;
    networkMonitor_ = networkMonitor;
    dispatcherQueue_ = dispatcherQueue;
    context_ = context;
    syncService_ = new SynchronizationService();
    cacheRefreshingService_ = new CacheRefreshingService();
    synchroSequencer_ = synchroSequencer;
  }

  private void startSynchronizationService() {
    synchronizationExecutor_.scheduleAtFixedRate(syncService_,
        new Random().nextLong() % SYNCHRONIZATION_PERIOD_SEC,
        SYNCHRONIZATION_PERIOD_SEC, TimeUnit.SECONDS);
  }

  private void startCacheRefreshingService() {
    cacheRefreshExecutor_.scheduleAtFixedRate(cacheRefreshingService_, CACHE_REFRESH_PERIOD_SEC,
        CACHE_REFRESH_PERIOD_SEC, TimeUnit.SECONDS);
  }

  /**
   * Service that is responsible for synchronization of asynchronous messages with another peers
   * from each synchro group current instance belongs to. It is run periodically.
   *
   * @author Piotr Malicki
   *
   */
  private class SynchronizationService implements Runnable {

    @Override
    public void run() {
      dispatcherQueue_.add(new JobInitMessage(new SynchronizeAsynchronousMessagesModule()));
    }
  }

  /**
   * Service that is responsible for refreshing cache of synchro-groups. It is run periodically.
   *
   * @author Piotr Malicki
   *
   */
  private class CacheRefreshingService implements Runnable {

    @Override
    public void run() {
      outQueue_.add(new JobInitMessage(new CacheRefreshingModule()));
    }

  }

  protected class AsyncMessagingModuleVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      try {
        context_.waitForInitialization();
      } catch (InterruptedException e) {
        logger_.warn("Interrupted while waiting for initialization of asynchronous messages " +
            "context.", e);
      }

      // Run peer selection module when new peer is found.
      MessageGenerator addFoundSynchroPeer = new MessageGenerator() {
        @Override
        public Message generate() {
          int lastPeerIndex = networkMonitor_.getKnownPeers().size() - 1;
          CommAddress lastPeer = networkMonitor_.getKnownPeers().get(lastPeerIndex);
          return new LastFoundPeerMessage(synchroSequencer_.getJobId(), lastPeer);
        }
      };

      networkMonitor_.addContextChangeMessageGenerator(addFoundSynchroPeer);

      startSynchronizationService();
      startCacheRefreshingService();
    }

    public void visit(EndModuleMessage message) {
      synchronizationExecutor_.shutdownNow();
      cacheRefreshExecutor_.shutdownNow();
      context_.shutDownServices();
      endJobModule();
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
