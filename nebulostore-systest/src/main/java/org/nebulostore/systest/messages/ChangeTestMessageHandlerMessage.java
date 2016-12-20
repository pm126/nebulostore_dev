package org.nebulostore.systest.messages;

import com.google.inject.Provider;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.NetworkMonitorForwarder;

/**
 * Sets a new provider of a handler of ConnectionTestMessage.
 *
 * @author szymon
 *
 */
public class ChangeTestMessageHandlerMessage extends Message {

  public ChangeTestMessageHandlerMessage(Provider<ConnectionTestMessageHandler> provider) {
    provider_ = provider;
  }

  private static final long serialVersionUID = 6643954942576792471L;

  private final Provider<ConnectionTestMessageHandler> provider_;

  public Provider<ConnectionTestMessageHandler> getProvider() {
    return provider_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new NetworkMonitorForwarder(this);
  }
}
