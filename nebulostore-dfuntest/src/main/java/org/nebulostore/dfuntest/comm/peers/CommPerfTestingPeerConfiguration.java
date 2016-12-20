package org.nebulostore.dfuntest.comm.peers;

import java.io.IOException;

import com.google.common.base.Function;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.nebulostore.dfuntest.comm.CommPerfTestHelperModule;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.peers.PeerConfiguration;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.persistence.SQLKeyValueStore;

public class CommPerfTestingPeerConfiguration extends PeerConfiguration {

  @Override
  protected void configure() {
    bind(CommPerfTestHelperModule.class).in(Scopes.SINGLETON);
    try {
      bind(new TypeLiteral<KeyValueStore<String>>() { }).
        annotatedWith(Names.named("AsyncTestHelperStore")).toInstance(
          new SQLKeyValueStore<String>(
            config_.getString("persistance.sql-keyvalue-store.host"),
            config_.getString("persistance.sql-keyvalue-store.port"),
            config_.getString("persistance.sql-keyvalue-store.database"),
            config_.getString("persistance.sql-keyvalue-store.user"),
            config_.getString("persistance.sql-keyvalue-store.password"), true,
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
    super.configure();
  }

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(CommPerfTestingPeer.class).in(Scopes.SINGLETON);
  }
}
