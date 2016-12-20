package org.nebulostore.dfuntest.coding.peers;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

import org.nebulostore.api.RecreateObjectFragmentsModule;
import org.nebulostore.api.WriteNebuloObjectModule;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.appcore.model.ObjectWriter;
import org.nebulostore.appcore.model.PartialObjectWriter;
import org.nebulostore.broker.AvailabilityBasedContractEvaluator;
import org.nebulostore.broker.Broker;
import org.nebulostore.broker.ContractsEvaluator;
import org.nebulostore.broker.ContractsSelectionAlgorithm;
import org.nebulostore.broker.GreedyContractsSelection;
import org.nebulostore.communication.CommunicationFacadeAdapterConfiguration;
import org.nebulostore.communication.routing.ListenerService;
import org.nebulostore.dfuntest.coding.CodingTestHelperModule;
import org.nebulostore.dfuntest.coding.CodingTestHelperModule.CodingTestResults;
import org.nebulostore.dfuntest.coding.api.GetNebuloObjectModuleWithSize;
import org.nebulostore.dfuntest.coding.api.RecreateObjectFragmentWithSizeModule;
import org.nebulostore.dfuntest.coding.api.WriteNebuloObjectPartsWithSize;
import org.nebulostore.dfuntest.coding.broker.AlwaysDenyingBroker;
import org.nebulostore.dfuntest.coding.broker.ContractsBreakingValuationBasedBroker;
import org.nebulostore.dfuntest.coding.communication.ListenerServiceAdapterWithSize;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.peers.PeerConfiguration;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.persistence.SQLKeyValueStore;

public class CodingAvailabilityTestingPeerConfiguration extends PeerConfiguration {

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(CodingAvailabilityTestingPeer.class).in(Scopes.SINGLETON);
  }

  @Override
  protected void configureCommunicationPeer() {
    AbstractModule absModule;
    absModule = new CommunicationFacadeAdapterConfiguration(config_);

    install(Modules.override(absModule).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(ListenerService.class).to(ListenerServiceAdapterWithSize.class);
      }
    }));
  }

  @Override
  protected void configureBroker() {
    if (config_.getString("dfuntest.mode").equals("server")) {
      bind(Broker.class).to(AlwaysDenyingBroker.class).in(Scopes.SINGLETON);
    } else {
      bind(Broker.class).to(ContractsBreakingValuationBasedBroker.class).in(Scopes.SINGLETON);
      bind(ContractsSelectionAlgorithm.class).to(GreedyContractsSelection.class);
      bind(ContractsEvaluator.class).to(AvailabilityBasedContractEvaluator.class);
    }
  }

  @Override
  protected void configureObjectGetters() {
    bind(ObjectGetter.class).to(GetNebuloObjectModuleWithSize.class);
  }

  @Override
  protected void configureObjectWriters() {
    bind(ObjectWriter.class).to(WriteNebuloObjectModule.class);
    bind(PartialObjectWriter.class).to(WriteNebuloObjectPartsWithSize.class);
    BlockingQueue<Message> calculatorInQueue = new LinkedBlockingQueue<>();
    bind(new TypeLiteral<BlockingQueue<Message>>() { }).annotatedWith(
        Names.named("CalculatorInQueue")).toInstance(calculatorInQueue);
  }

  @Override
  protected void configureErasureCoding() {
    super.configureErasureCoding();
    bind(RecreateObjectFragmentsModule.class).to(RecreateObjectFragmentWithSizeModule.class);
  }

  @Override
  protected void configureAdditional() {
    super.configureAdditional();
    configureCodingTestHelper();
  }

  private void configureCodingTestHelper() {
    bind(CodingTestHelperModule.class).in(Scopes.SINGLETON);
    try {
      bind(new TypeLiteral<KeyValueStore<CodingTestResults>>() { }).
      annotatedWith(Names.named("CodingTestHelperStore")).toInstance(
          new SQLKeyValueStore<CodingTestResults>(
            config_.getString("persistance.sql-keyvalue-store.host"),
            config_.getString("persistance.sql-keyvalue-store.port"),
            config_.getString("persistance.sql-keyvalue-store.database"),
            config_.getString("persistance.sql-keyvalue-store.user"),
            config_.getString("persistance.sql-keyvalue-store.password"), true,
            new Function<CodingTestResults, byte[]>() {
              @Override
              public byte[] apply(CodingTestResults input) {
                return new GsonBuilder().enableComplexMapKeySerialization().create().toJson(input).
                    getBytes();
              }
            }, new Function<byte[], CodingTestResults>() {
                @Override
                public CodingTestResults apply(byte[] input) {
                  return new Gson().fromJson(new String(input), CodingTestResults.class);
                }
              }));
    } catch (IOException e) {
      throw new RuntimeException("Unable to connect to database.", e);
    }

  }
}
