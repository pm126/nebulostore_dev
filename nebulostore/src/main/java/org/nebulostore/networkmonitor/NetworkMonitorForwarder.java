package org.nebulostore.networkmonitor;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

import static com.google.common.base.Preconditions.checkNotNull;
/**
 * Forwards messages to NetworkMonitor.
 * @author szymon
 *
 */
public class NetworkMonitorForwarder extends JobModule {
  private static Logger logger_ = Logger.getLogger(NetworkMonitorForwarder.class);

  private NetworkMonitor networkMonitor_;
  private final Message message_;

  public NetworkMonitorForwarder(Message message) {
    message_ = message;
  }

  @Inject
  private void setNetworkMonitor(NetworkMonitor networkMonitor) {
    networkMonitor_ = checkNotNull(networkMonitor);
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  @Override
  protected void initModule() {
    logger_.debug("Forwarding message " + message_.getClass().getName() + " to NetworkMonitor.");
    networkMonitor_.getInQueue().add(message_);
    endJobModule();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
  }

}
