package org.nebulostore.dfuntest.async;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.async.utils.MessagesInfoDeserializer;
import org.nebulostore.dfuntest.async.utils.MessagesInfoSerializer;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.timer.Timer;

/**
 * Helper module for asynchronous messages functional test. It sends messages to the analogous
 * modules at the other peers and gathers information about sent and received messages.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncTestHelperModule extends JobModule {
  public static final String ASYNC_HELPER_SENT_PREFIX = "AsyncHelperSentMessages";
  public static final String ASYNC_HELPER_RECEIVED_PREFIX = "AsyncHelperReceivedMessages";
  public static final String ASYNC_HELPER_LOGIN_COUNT_PREFIX = "AsyncHelperLoginCount";
  public static final String ASYNC_HELPER_LOGIN_MAP_PREFIX = "AsyncHelperLoginMap";
  public static final String ASYNC_HELPER_DELAYS_PREFIX = "AsyncHelperDelays";

  private static Logger logger_ = Logger.getLogger(AsyncTestHelperModule.class);

  protected static final int MIN_MESSAGE_SEND_PERIOD_MILIS = 30000;
  protected static final int MAX_MESSAGE_SEND_PERIOD_MILIS = 90000;
  protected static final int NUMBER_OF_PEERS_TO_CHECK = 10;

  private final MessageVisitor visitor_ = new AsyncTestHelperMessageVisitor();

  private final Map<CommAddress, Set<String>> sentMessagesIds_ = new ConcurrentHashMap<>();
  private final Map<CommAddress, Set<String>> receivedMessagesIds_ = new ConcurrentHashMap<>();
  private final Map<String, Long> messagesDelays_ = new ConcurrentHashMap<>();

  private final Timer timer_;
  private final NetworkMonitor networkMonitor_;
  private final CommAddress myAddress_;
  private final String testId_;

  private final KeyValueStore<String> store_;
  private final int loginCount_;
  private final Map<Integer, LoginData> loginTimestamps_ = new HashMap<>();

  private boolean stoppedSending_ = true;

  @Inject
  public AsyncTestHelperModule(Timer timer, NetworkMonitor networkMonitor, CommAddress myAddress,
      @Named("AsyncTestHelperStore") KeyValueStore<String> store,
      @Named("test-id") String testId) {
    timer_ = timer;
    networkMonitor_ = networkMonitor;
    myAddress_ = myAddress;
    store_ = store;
    testId_ = testId;

    String loginCountString = store_.get(ASYNC_HELPER_LOGIN_COUNT_PREFIX + testId_ + myAddress_);
    logger_.debug("Downloaded login string: " + loginCountString);
    int loginCount = -1;
    if (loginCountString != null) {
      loginCount = Integer.parseInt(loginCountString);
    }
    loginCount_ = loginCount + 1;
    try {
      logger_.debug("Putting login count into database: " + loginCount_);
      store_.put(ASYNC_HELPER_LOGIN_COUNT_PREFIX + testId_ + myAddress_,
          Integer.toString(loginCount_));
    } catch (IOException e) {
      logger_.warn("Could not put login count into the database", e);
    }

    loginTimestamps_.put(loginCount_, new LoginData(System.currentTimeMillis(), null));
    logger_.debug("Putting new timestamp: " + loginTimestamps_.get(loginCount_));
    String mapString = store_.get(ASYNC_HELPER_LOGIN_MAP_PREFIX + testId_ + myAddress_);
    logger_.debug("Downloaded login timestamps: " + mapString);
    if (mapString != null) {
      Type type = (new TypeToken<Map<Integer, LoginData>>() { }).getType();
      Map<Integer, LoginData> loginTimestamps = new Gson().fromJson(mapString, type);
      loginTimestamps_.putAll(loginTimestamps);
    }
    try {
      logger_.debug("Updating timestamps in the database: " + loginTimestamps_);
      store_.put(ASYNC_HELPER_LOGIN_MAP_PREFIX + testId_ + myAddress_,
          new Gson().toJson(loginTimestamps_));
    } catch (IOException e) {
      logger_.warn("Could not update timestamps in the database", e);
    }

  }
  public Map<CommAddress, Set<String>> getSentMessagesIds() {
    return sentMessagesIds_;
  }

  public Map<CommAddress, Set<String>> getReceivedMessagesIds() {
    return receivedMessagesIds_;
  }

  public Map<String, Long> getMessagesDelays() {
    return messagesDelays_;
  }

  public void startMessagesSending() {
    logger_.debug("Starting messages sending, inQueue: " + inQueue_);
    logger_.debug("Got synchronized");

    scheduleNextSendAction();
    stoppedSending_ = false;
    logger_.debug("Started sending");
  }

  public void stopMessagesSending() {
    logger_.debug("Stopping messages sending, inQueue: " + inQueue_);
    logger_.debug("Got synchronized");
    stoppedSending_ = true;
    logger_.debug("Cancelled timer");
  }

  public void shutDown() {
    //FIXME jakaś synchronizacja? Pewnie też AtomicBoolean...
    loginTimestamps_.get(loginCount_).endingTime_ = System.currentTimeMillis();
    stoppedSending_ = true;
    timer_.cancelTimer();
    try {
      store_.put(ASYNC_HELPER_LOGIN_MAP_PREFIX + testId_ + myAddress_,
          new Gson().toJson(loginTimestamps_));
    } catch (IOException e) {
      logger_.debug("Failed to save login ending time.", e);
    }
    endJobModule();
  }

  private void scheduleNextSendAction() {
    //FIXME może tylko jeden random na klasę?
    Random random = new Random();
    int period = random.nextInt(MAX_MESSAGE_SEND_PERIOD_MILIS - MIN_MESSAGE_SEND_PERIOD_MILIS) +
        MIN_MESSAGE_SEND_PERIOD_MILIS;
    timer_.schedule(new AsyncTestHelperMessage(jobId_), period);
  }

  protected class AsyncTestHelperMessageVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {

      String sentMsgsString = store_.get(ASYNC_HELPER_SENT_PREFIX + testId_ + myAddress_);
      if (sentMsgsString != null) {
        logger_.debug("Adding sent messages from the database: " + sentMsgsString);
        sentMessagesIds_.putAll(new MessagesInfoDeserializer().apply(sentMsgsString));
      }

      String receivedMsgsString = store_.get(ASYNC_HELPER_RECEIVED_PREFIX + testId_ + myAddress_);
      if (receivedMsgsString != null) {
        logger_.debug("Adding received messages from the database: " + receivedMsgsString);
        receivedMessagesIds_.putAll(new MessagesInfoDeserializer().apply(receivedMsgsString));
      }

      Type type = (new TypeToken<Map<String, Long>>() { }).getType();
      String msgsDelaysString = store_.get(ASYNC_HELPER_DELAYS_PREFIX + testId_ + myAddress_);
      if (msgsDelaysString != null) {
        logger_.debug("Delays string: " + msgsDelaysString);
        Map<String, Long> messagesDelays = new Gson().fromJson(msgsDelaysString, type);
        logger_.debug("Delays MAP: " + messagesDelays);
        messagesDelays_.putAll(messagesDelays);
      }
    }

    public void visit(AsyncTestHelperMessage message) {
      if (!stoppedSending_) {
        List<CommAddress> peers = Lists.newArrayList(networkMonitor_.getKnownPeers());
        Collections.shuffle(peers);
        for (CommAddress peer : peers.subList(0,
            Math.min(NUMBER_OF_PEERS_TO_CHECK, peers.size()))) {
          if (!peer.equals(myAddress_)) {
            logger_.debug("Downloading login count for peer:" + peer);
            String logCountString = store_.get(ASYNC_HELPER_LOGIN_COUNT_PREFIX + testId_ + peer);
            logger_.debug("Downloaded login count: " + logCountString);
            int loginCount = -1;
            if (logCountString != null) {
              loginCount = Integer.parseInt(logCountString);
            }
            Message msg = new AsyncTestMessage(myAddress_, peer, loginCount);
            logger_.debug("Sending next test message: " + msg);
            networkQueue_.add(msg);
            if (!sentMessagesIds_.containsKey(peer)) {
              sentMessagesIds_.put(peer, new HashSet<String>());
            }
            sentMessagesIds_.get(peer).add(msg.getMessageId());
          }
        }
        scheduleNextSendAction();
        updateInfoInStore();
      }
    }

    public void visit(AsyncTestMessage message) {
      addReceivedMessage(message.getSourceAddress(), message.getMessageId());
    }

    public void visit(AsyncTestAsynchronousMessage message) {
      logger_.debug("Received a message that was sent asynchronously: " + message);
      if (receivedMessagesIds_.containsKey(message.getOriginalMessageId())) {
        logger_.info("Message with id " + message.getOriginalMessageId() + " has been already" +
            "received");
        return;
      }

      logger_.debug("Login: " + message.getReceiverLoginCount() + " current logins: " +
          loginTimestamps_ + " current delays: " + messagesDelays_);
      logger_.debug("Next async message: " + message.getOriginalMessageId());
      if (loginTimestamps_.containsKey(message.getReceiverLoginCount() + 1)) {
        long waitingTime = 0L;
        for (int i = message.getReceiverLoginCount() + 1; i < loginCount_; i++) {
          LoginData loginData = loginTimestamps_.get(i);
          waitingTime += loginData.endingTime_ - loginData.startingTime_;
        }
        waitingTime += System.currentTimeMillis() - loginTimestamps_.get(loginCount_).startingTime_;
        messagesDelays_.put(message.getOriginalMessageId(), waitingTime);
        logger_.debug("Added new message delay, current map: " + messagesDelays_);
      } else {
        logger_.warn("Received an asynchronous message that was probably sent when current " +
            "instance was available.");
      }
      addReceivedMessage(message.getRecipient(), message.getOriginalMessageId());
    }

    private void addReceivedMessage(CommAddress sourceAddress, String msgId) {
      logger_.debug("Adding received message : " + msgId);
      if (!receivedMessagesIds_.containsKey(sourceAddress)) {
        receivedMessagesIds_.put(sourceAddress, new HashSet<String>());
      }
      receivedMessagesIds_.get(sourceAddress).add(msgId);
      updateInfoInStore();
    }

    private void updateInfoInStore() {
      if (inQueue_.size() == 0) {
        logger_.debug("Updating info in store");
        try {
          MessagesInfoSerializer serializer = new MessagesInfoSerializer();
          store_.put(ASYNC_HELPER_SENT_PREFIX + testId_ + myAddress_,
              serializer.apply(sentMessagesIds_));
          store_.put(ASYNC_HELPER_RECEIVED_PREFIX + testId_ + myAddress_,
              serializer.apply(receivedMessagesIds_));
          store_.put(ASYNC_HELPER_DELAYS_PREFIX + testId_ + myAddress_,
              new Gson().toJson(messagesDelays_));
        } catch (IOException e) {
          logger_.warn("Error while trying to save info in store", e);
        } finally {
          logger_.debug("Updating finished");
        }
      }
    }
  }

  private class AsyncTestHelperMessage extends Message {

    private static final long serialVersionUID = 1154697027121390889L;

    public AsyncTestHelperMessage(String jobId) {
      super(jobId);
    }

  }

  private class LoginData {

    public Long startingTime_;
    public Long endingTime_;

    public LoginData(Long startingTime, Long endingTime) {
      startingTime_ = startingTime;
      endingTime_ = endingTime;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
