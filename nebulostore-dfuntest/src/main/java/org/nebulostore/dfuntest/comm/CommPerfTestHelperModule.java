package org.nebulostore.dfuntest.comm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommPerfTestHelperModule extends JobModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommPerfTestHelperModule.class);

  private static final long MESSAGE_SEND_DELAY_MILIS = 1000;

  private final MessageVisitor visitor_ = new CommPerfTestVisitor();
  private final Timer timer_;
  private final NetworkMonitor networkMonitor_;
  private final CommAddress myAddress_;
  private final Map<CommAddress, Set<String>> sentMessages_ = new HashMap<>();
  private final Map<CommAddress, Set<String>> receivedMessages_ = new HashMap<>();

  @Inject
  public CommPerfTestHelperModule(Timer timer, NetworkMonitor networkMonitor,
      CommAddress myAddress) {
    timer_ = timer;
    networkMonitor_ = networkMonitor;
    myAddress_ = myAddress;
  }

  public Map<CommAddress, Set<String>> getReceivedMessagesIds() {
    // LOGGER.debug("Returning received messages map: " + receivedMessages_);
    return receivedMessages_;
  }

  public Map<CommAddress, Set<String>> getSentMessagesIds() {
    // LOGGER.debug("Returning sent messages map: " + sentMessages_);
    return sentMessages_;
  }

  public void stopMessagesSending() {
    timer_.cancelTimer();
  }

  public void shutDown() {
    timer_.cancelTimer();
    endJobModule();
  }

  protected class CommPerfTestVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      if (myAddress_.equals(CommAddress.getZero())) {
        timer_.scheduleRepeated(new TestMessage(jobId_), MESSAGE_SEND_DELAY_MILIS,
            MESSAGE_SEND_DELAY_MILIS);
      }
    }

    public void visit(TestMessage message) {
      if (myAddress_.equals(CommAddress.getZero())) {
        LOGGER.debug("Starting next test iteration");
        for (CommAddress peer : networkMonitor_.getKnownPeers()) {
          LOGGER.debug("Next known peer: " + peer);
          if (!peer.equals(myAddress_)) {
            for (int i = 0; i < 1; i++) {
              Message msg = new CommPerfTestMessage(myAddress_, peer);
              if (!sentMessages_.containsKey(peer)) {
                sentMessages_.put(peer, new HashSet<String>());
              }
              sentMessages_.get(peer).add(msg.getMessageId());
              networkQueue_.add(msg);
              LOGGER.debug("Sent next message to peer: " + peer + ", sentMessages size: " +
                  sentMessages_.get(peer).size());
            }
          }
        }
      }
    }

    public void visit(CommPerfTestMessage message) {
      LOGGER.debug("Received test message from " + message.getSourceAddress());
      if (!receivedMessages_.containsKey(message.getSourceAddress())) {
        receivedMessages_.put(message.getSourceAddress(), new HashSet<String>());
      }
      receivedMessages_.get(message.getSourceAddress()).add(message.getMessageId());
    }

  }

  private static class TestMessage extends Message {

    private static final long serialVersionUID = 4997286286294043532L;

    public TestMessage(String jobId) {
      super(jobId);
    }

  }

  private static class CommPerfTestMessage extends CommMessage {

    private static final long serialVersionUID = 2898511924304880527L;

    public CommPerfTestMessage(CommAddress sourceAddress, CommAddress destAddress) {
      super(sourceAddress, destAddress);
    }

    @Override
    public JobModule getHandler() throws NebuloException {
      return new CommPerfTestHelperMessageForwarder(this);
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
