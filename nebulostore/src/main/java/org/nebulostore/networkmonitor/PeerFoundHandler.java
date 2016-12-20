package org.nebulostore.networkmonitor;
import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommPeerFoundMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module handle CommPeerFoundMessage.
 * @author szymonmatejczyk
 */
public class PeerFoundHandler extends JobModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(PeerFoundHandler.class);
  private final PeerFoundHandlerVisitor visitor_ = new PeerFoundHandlerVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private NetworkMonitor networkMonitor_;

  @Inject
  public void setDependencies(NetworkMonitor networkMonitor) {
    networkMonitor_ = networkMonitor;
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  /**
   * @author szymonmatejczyk
   */
  protected class PeerFoundHandlerVisitor extends MessageVisitor {
    public void visit(CommPeerFoundMessage message) {
      jobId_ = message.getId();
      LOGGER.info("Adding new found peer: " + message.getSourceAddress());
      networkMonitor_.addFoundPeer(message.getSourceAddress());
      endJobModule();
    }
  }

}
