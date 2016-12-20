package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;
import org.nebulostore.communication.naming.CommAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object transfers messages to their destination. Either by sending it to network
 * destination or by listening for messages and directing them to correct listener.
 *
 * For each incoming message every message listener that matches that message receives it.
 *
 * On shutdown no new messages are dispatched.
 *
 * This is a service that should be started before use and shutdown when no longer useful.
 *
 * @author Grzegorz Milka
 *
 */
public class Router implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Router.class);
  private final ListenerService listenerService_;
  private final MessageSender msgSender_;
  private final Map<MessageListener, MessageMatcher> listeners_;
  private final ExecutorService executor_;

  private final CommAddress localCommAddress_;

  private Future<?> thisFuture_;

  @Inject
  public Router(
      ListenerService listener,
      MessageSender sender,
      @Named("communication.local-comm-address") CommAddress commAddress,
      @Named("communication.routing.router-executor") ExecutorService executor) {
    listenerService_ = listener;
    msgSender_ = sender;
    listeners_ = new ConcurrentHashMap<>();
    executor_ = executor;

    localCommAddress_ = commAddress;
  }

  public void addMessageListener(MessageMatcher matcher, MessageListener listener) {
    LOGGER.trace("addMessageListener({}, {})", matcher, listener);
    listeners_.put(listener, matcher);
  }

  public void removeMessageListener(MessageListener listener) {
    LOGGER.trace("removeMessageListener({})", listener);
    listeners_.remove(listener);
  }

  @Override
  public void run() {
    BlockingQueue<CommMessage> queue = listenerService_.getListeningQueue();
    while (true) {
      CommMessage msg;
      try {
        msg = queue.take();
        LOGGER.debug("Received message: {}", msg);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted!!! ", e);
        break;
      }
      dispatchMessage(msg);
    }
  }

  /**
   * Send message over network.
   *
   * @see MessageSender
   *
   * @param msg
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public MessageSendFuture sendMessage(CommMessage msg) {
    attachLocalCommAddress(msg);
    return msgSender_.sendMessage(msg);
  }

  /**
   * Send message over network and add result to queue.
   *
   * @see MessageSender
   *
   * @param msg
   * @param resultQueue
   *          queue to which add result
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *         {@code InterruptedException} and {@code IOException}.
   */
  public MessageSendFuture sendMessage(CommMessage msg, BlockingQueue<SendResult> resultQueue) {
    attachLocalCommAddress(msg);
    return msgSender_.sendMessage(msg, resultQueue);
  }

  public void startUp() throws IOException {
    LOGGER.debug("startUp()");
    thisFuture_ = executor_.submit(this);
    listenerService_.startUp();
    msgSender_.startUp();
  }

  public void shutDown() throws InterruptedException {
    LOGGER.debug("shutDown()");
    msgSender_.shutDown();
    listenerService_.shutDown();
    try {
      thisFuture_.get(1, TimeUnit.SECONDS);
    } catch (ExecutionException | TimeoutException e) {
      LOGGER.error("Exception", e);
    }
    thisFuture_.cancel(true);
    LOGGER.debug("shutDown(): void");
  }

  private void attachLocalCommAddress(CommMessage msg) {
    msg.setSourceAddress(localCommAddress_);
  }

  private void dispatchMessage(CommMessage msg) {
    for (Map.Entry<MessageListener, MessageMatcher> entry: listeners_.entrySet()) {
      LOGGER.debug("Matching {} with {}, result: {}", new Object[] {msg, entry.getValue(),
          entry.getValue().matchMessage(msg)});
      if (entry.getValue().matchMessage(msg)) {
        entry.getKey().onMessageReceive(msg);
      }
    }
  }
}
