package org.nebulostore.systest;

import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

import org.nebulostore.broker.Broker;
import org.nebulostore.broker.ContractsEvaluator;
import org.nebulostore.broker.ContractsSelectionAlgorithm;
import org.nebulostore.broker.GreedyContractsSelection;
import org.nebulostore.broker.OnlySizeContractsEvaluator;
import org.nebulostore.crypto.BasicEncryptionAPI;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.DefaultConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.peers.PeerConfiguration;
import org.nebulostore.systest.broker.ValuationBasedBrokerWithContractsOpened;
import org.nebulostore.systest.readwrite.ReadWriteClientFactory;
import org.nebulostore.systest.readwrite.ReadWriteClientFactoryDefaultImpl;

/**
 * Configuration for Testing Peer.
 * @author Bolek Kulbabinski
 */
public class TestingPeerConfiguration extends PeerConfiguration {

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(TestingPeer.class).in(Scopes.SINGLETON);
  }

  @Override
  protected void configureEncryption() {
    bind(EncryptionAPI.class).to(BasicEncryptionAPI.class).in(Scopes.SINGLETON);
    bind(String.class).annotatedWith(
        Names.named("PublicKeyPeerId")).toInstance("");
    bind(String.class).annotatedWith(
        Names.named("PrivateKeyPeerId")).toInstance("");
  }

  @Override
  protected void configureAdditional() {
    bind(ReadWriteClientFactory.class).to(ReadWriteClientFactoryDefaultImpl.class).in(
        Singleton.class);
  }

  @Override
  protected void configureNetworkMonitor() {
    bind(NetworkMonitor.class).to(NetworkMonitorForTesting.class).in(Scopes.SINGLETON);
    bind(ConnectionTestMessageHandler.class).to(DefaultConnectionTestMessageHandler.class);
  }

  @Override
  protected void configureBroker() {
    bind(Broker.class).to(ValuationBasedBrokerWithContractsOpened.class).in(Scopes.SINGLETON);
    bind(ContractsSelectionAlgorithm.class).to(GreedyContractsSelection.class);
    bind(ContractsEvaluator.class).to(OnlySizeContractsEvaluator.class);
  }
}
