package org.nebulostore.systest.readwrite;

import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.nebulostore.systest.TestingPeerConfiguration;

/**
 * @author hryciukrafal
 */
public class ReadWriteWithTimeConfiguration extends TestingPeerConfiguration {

  @Override
  protected void configureAdditional() {
    bind(ReadWriteClientFactory.class).to(ReadWriteTimeClientFactory.class).in(Singleton.class);
    bind(String.class).annotatedWith(Names.named("data-file"))
        .toInstance(config_.getString("systest.data-file"));
  }

}
