package org.nebulostore.communication;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.communication.bootstrap.BootstrapService;
import org.nebulostore.communication.bootstrap.UserGivenBootstrapService;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressMappingMaintainer;
import org.nebulostore.communication.naming.CachedCommAddressResolver;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.naming.CommAddressResolver;
import org.nebulostore.communication.naming.SimpleCommAddressResolver;
import org.nebulostore.communication.naming.addressmap.AddressMapFactory;
import org.nebulostore.communication.naming.addressmap.AddressMapFactoryImpl;
import org.nebulostore.communication.netutils.NetworkAddressDiscovery;
import org.nebulostore.communication.netutils.UserGivenNetworkAddressDiscovery;
import org.nebulostore.communication.netutils.remotemap.RemoteMap;
import org.nebulostore.communication.netutils.remotemap.RemoteMapClientFactory;
import org.nebulostore.communication.netutils.remotemap.RemoteMapFactory;
import org.nebulostore.communication.netutils.remotemap.RemoteMapImpl;
import org.nebulostore.communication.netutils.remotemap.RemoteMapServerFactory;
import org.nebulostore.communication.peerdiscovery.PeerDiscovery;
import org.nebulostore.communication.peerdiscovery.PeerDiscoveryFactory;
import org.nebulostore.communication.peerdiscovery.SamplingGossipPeerDiscovery;
import org.nebulostore.communication.routing.ByteListenerService;
import org.nebulostore.communication.routing.ByteSender;
import org.nebulostore.communication.routing.ListenerService;
import org.nebulostore.communication.routing.ListenerServiceAdapter;
import org.nebulostore.communication.routing.MessageSender;
import org.nebulostore.communication.routing.MessageSenderAdapter;
import org.nebulostore.communication.routing.Router;
import org.nebulostore.communication.routing.plainsocket.PlainSocketByteListenerService;
import org.nebulostore.communication.routing.plainsocket.PlainSocketByteSender;
import org.nebulostore.persistence.InMemoryStore;

/**
 * @author Grzegorz Milka
 */
public class CommunicationFacadeConfiguration extends AbstractModule {
  private static final int N_SERVICES = 4;
  protected final XMLConfiguration xmlConfig_;
  protected final ExecutorService serviceExecutor_ = Executors.newFixedThreadPool(N_SERVICES);

  public CommunicationFacadeConfiguration(
      XMLConfiguration xmlConfig) {
    xmlConfig_ = xmlConfig;
  }

  @Override
  protected void configure() {
    CommAddress localCommAddress = new CommAddress(
        xmlConfig_.getString("communication.comm-address", ""));

    bind(CommAddress.class).annotatedWith(Names.named("communication.local-comm-address")).
      toInstance(localCommAddress);
    configureBootstrap();
    configureNaming();
    configurePeerDiscovery();
    configureRemoteMap();
    configureRouting();

    bind(NetworkAddressDiscovery.class).to(UserGivenNetworkAddressDiscovery.class);
    bind(ExecutorService.class).annotatedWith(Names.named("communication.service-executor")).
      toInstance(serviceExecutor_);

  }

  private void configureBootstrap() {
    bind(CommAddress.class).annotatedWith(Names.named("communication.bootstrap-comm-address")).
        toInstance(new CommAddress(xmlConfig_.getString("communication.bootstrap-comm-address")));
    bind(BootstrapService.class).to(UserGivenBootstrapService.class);
  }

  private void configureNaming() {
    InetSocketAddress localInetSocketAddress;
    try {
      localInetSocketAddress = new InetSocketAddress(
          InetAddress.getByName(xmlConfig_.getString("communication.local-net-address")),
          xmlConfig_.getInt("communication.ports.comm-cli-port"));
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unexpected exception.", e);
    }
    bind(InetSocketAddress.class).annotatedWith(
        Names.named("communication.local-netsocket-address")).toInstance(localInetSocketAddress);

    bind(ScheduledExecutorService.class).annotatedWith(Names.named(
        "communication.address-map-maintainer-scheduled-executor")).
        toInstance(Executors.newScheduledThreadPool(1));
    bindConstant().annotatedWith(Names.named("communication.address-map-check-interval")).to(
        xmlConfig_.getInt("communication.address-map-check-interval"));
    bind(TimeUnit.class).annotatedWith(Names.named(
        "communication.address-map-check-interval-unit")).toInstance(TimeUnit.MILLISECONDS);
    bind(AddressMappingMaintainer.class);

    bind(AddressMapFactory.class).to(AddressMapFactoryImpl.class).in(Singleton.class);

    bind(CommAddressResolver.class).annotatedWith(Names.named(
        "communication.naming.cached-base-resolver")).to(
            SimpleCommAddressResolver.class).in(Singleton.class);
    bind(CommAddressResolver.class).to(CachedCommAddressResolver.class).in(Singleton.class);
    bind(ScheduledExecutorService.class).annotatedWith(
        Names.named("communication.naming.cached-scheduled-executor")).toInstance(
        Executors.newScheduledThreadPool(1));
  }

  protected void configurePeerDiscovery() {
    ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    bind(ExecutorService.class).annotatedWith(Names.named(
        "communication.peerdiscovery.service-executor")).
        toInstance(serviceExecutor_);
    bind(ScheduledExecutorService.class).annotatedWith(Names.named(
        "communication.peerdiscovery.scheduled-executor")).
        toInstance(scheduledExecutor);
    install(new FactoryModuleBuilder().implement(PeerDiscovery.class,
          SamplingGossipPeerDiscovery.class).build(PeerDiscoveryFactory.class));
  }

  private void configureRemoteMap() {
    ExecutorService workerExecutor = Executors.newFixedThreadPool(4);
    if (xmlConfig_.getString("communication.remotemap.mode").equals("server")) {
      // TODO: add config entry for either in-memory or persistent map
      bind(RemoteMap.class).toInstance(new RemoteMapImpl(new InMemoryStore<Serializable>()));
      bind(ExecutorService.class).annotatedWith(Names.named(
          "communication.remotemap.server-executor")).toInstance(serviceExecutor_);
      bind(ExecutorService.class).annotatedWith(Names.named(
          "communication.remotemap.worker-executor")).toInstance(workerExecutor);
      bindConstant().annotatedWith(Names.named("communication.remotemap.local-port")).to(
          xmlConfig_.getString("communication.remotemap.local-port"));
      bind(RemoteMapFactory.class).to(RemoteMapServerFactory.class).in(Singleton.class);
    } else {
      try {
        bind(InetSocketAddress.class).annotatedWith(Names.named(
            "communication.remotemap.server-net-address")).toInstance(
                new InetSocketAddress(InetAddress.getByName(xmlConfig_.getString(
                    "communication.remotemap.server-net-address")),
                    xmlConfig_.getInt("communication.remotemap.server-port")));
      } catch (UnknownHostException e) {
        throw new IllegalStateException("Unexpected exception.", e);
      }
      bind(RemoteMapFactory.class).to(RemoteMapClientFactory.class).in(Singleton.class);
    }
  }

  private void configureRouting() {
    BlockingQueue<byte[]> byteListeningQueue = new LinkedBlockingQueue<>();
    BlockingQueue<CommMessage> listeningQueue = new LinkedBlockingQueue<>();
    ExecutorService listenerWorkerExecutor = Executors.newCachedThreadPool();
    ExecutorService senderWorkerExecutor = Executors.newFixedThreadPool(2);
    ExecutorService byteSenderWorkerExecutor = Executors.newFixedThreadPool(4);

    bindConstant().annotatedWith(Names.named("communication.ports.comm-cli-port")).to(
        xmlConfig_.getInt("communication.ports.comm-cli-port"));
    bind(new TypeLiteral<BlockingQueue<CommMessage>>() { }).
      annotatedWith(Names.named("communication.routing.listening-queue")).
      toInstance(listeningQueue);
    bind(new TypeLiteral<BlockingQueue<byte[]>>() { }).
      annotatedWith(Names.named("communication.routing.byte-listening-queue")).
      toInstance(byteListeningQueue);
    bind(Executor.class).annotatedWith(Names.named(
        "communication.routing.listener-service-executor")).
      toInstance(serviceExecutor_);
    bind(ExecutorService.class).annotatedWith(Names.named(
        "communication.routing.listener-worker-executor")).
      toInstance(listenerWorkerExecutor);

    bind(ByteListenerService.class).to(PlainSocketByteListenerService.class).in(Singleton.class);
    bind(ListenerService.class).to(ListenerServiceAdapter.class);

    bind(ExecutorService.class).annotatedWith(Names.named(
        "communication.routing.sender-worker-executor")).
        toInstance(senderWorkerExecutor);
    bind(ExecutorService.class).annotatedWith(Names.named(
        "communication.routing.byte-sender-worker-executor")).
        toInstance(byteSenderWorkerExecutor);

    bind(ByteSender.class).to(PlainSocketByteSender.class).in(Singleton.class);
    bind(MessageSender.class).to(MessageSenderAdapter.class);

    bind(ExecutorService.class).annotatedWith(Names.named(
        "communication.routing.router-executor")).
      toInstance(serviceExecutor_);
    bind(Router.class).in(Singleton.class);
  }
}
