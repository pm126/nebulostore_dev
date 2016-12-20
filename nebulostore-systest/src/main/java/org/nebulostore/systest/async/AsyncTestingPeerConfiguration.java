package org.nebulostore.systest.async;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.async.synchrogroup.selector.AlwaysDenyingSynchroPeerSelector;
import org.nebulostore.async.synchrogroup.selector.SynchroPeerSelector;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.systest.TestingPeerConfiguration;

public class AsyncTestingPeerConfiguration extends TestingPeerConfiguration {

  private static Logger logger_ = Logger.getLogger(AsyncTestingPeerConfiguration.class);

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(AsyncTestingPeer.class).in(Scopes.SINGLETON);
  }

  @Override
  protected void configureAsyncSelector() {
    bind(SynchroPeerSelector.class).toInstance(new AlwaysDenyingSynchroPeerSelector());
  }

  @Override
  protected void configureAdditional() {
    super.configureAdditional();
    bind(CounterModule.class).toInstance(new CounterModule());
    logger_.debug("Additional modules configured.");
  }

  @Override
  protected void configureQueues() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Message> dispatcherQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Message> commPeerInQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Message> msgRcvCheckerInQueue = new LinkedBlockingQueue<>();

    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("NetworkQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommunicationPeerInQueue")).toInstance(commPeerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("DispatcherQueue")).toInstance(dispatcherQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommunicationPeerOutQueue")).toInstance(msgRcvCheckerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommOverlayInQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommOverlayNetworkQueue")).toInstance(msgRcvCheckerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("CommOverlayOutQueue")).toInstance(dispatcherQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("MsgReceivingCheckerInQueue")).toInstance(msgRcvCheckerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("MsgReceivingCheckerNetworkQueue")).toInstance(commPeerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).
      annotatedWith(Names.named("MsgReceivingCheckerOutQueue")).toInstance(networkQueue);
    logger_.debug("Communication queues configured.");
  }
}
