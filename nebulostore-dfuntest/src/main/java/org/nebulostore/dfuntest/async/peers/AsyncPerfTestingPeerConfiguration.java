package org.nebulostore.dfuntest.async.peers;

import java.io.IOException;

import com.google.common.base.Function;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.nebulostore.dfuntest.async.AsyncTestHelperModule;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.peers.PeerConfiguration;
import org.nebulostore.persistence.FileStore;
import org.nebulostore.persistence.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Peer configuration for asynchronous messages performance test.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncPerfTestingPeerConfiguration extends PeerConfiguration {

  private static Logger logger_ = LoggerFactory.getLogger(AsyncPerfTestingPeerConfiguration.class);

  @Override
  protected void configure() {
    logger_.info("Starting configuration");
    super.configure();
    logger_.info("Configured regular peer");
    configureAsyncTestHelper();
    logger_.info("Configured async test helper");
  }

  @Override
  protected void configurePeer() {
    logger_.info("Configuring peer");
    bind(AbstractPeer.class).to(AsyncPerfTestingPeer.class).in(Scopes.SINGLETON);
    logger_.info("Peer configured");
  }

  private void configureAsyncTestHelper() {
    logger_.info("Binding test helper");
    bind(AsyncTestHelperModule.class).in(Scopes.SINGLETON);
    logger_.info("Test helper bound");
    try {
      bind(new TypeLiteral<KeyValueStore<String>>() { }).
        annotatedWith(Names.named("AsyncTestHelperStore")).toInstance(
          new FileStore<String>("asynctest",
          /*new SQLKeyValueStore<String>(
            config_.getString("persistance.sql-keyvalue-store.host"),
            config_.getString("persistance.sql-keyvalue-store.port"),
            config_.getString("persistance.sql-keyvalue-store.database"),
            config_.getString("persistance.sql-keyvalue-store.user"),
            config_.getString("persistance.sql-keyvalue-store.password"), true,*/
            new Function<String, byte[]>() {
              @Override
              public byte[] apply(String input) {
                return input.getBytes();
              }
            }, new Function<byte[], String>() {
                @Override
                public String apply(byte[] input) {
                  return new String(input);
                }
              }));
    } catch (IOException e) {
      throw new RuntimeException("Unable to connect to database.", e);
    }
    logger_.info("Database bound");
  }

}
