package org.nebulostore.dfuntest.async;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.NebulostoreApp;
import org.nebulostore.dfuntest.NebulostoreReliabilityTestScript;
import org.nebulostore.dfuntest.async.utils.MessagesInfoDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulostoreAsyncTestScript extends NebulostoreReliabilityTestScript {
  private static final Logger LOGGER = LoggerFactory.getLogger(NebulostoreAsyncTestScript.class);

  private static final String ASYNC_TEST_SCRIPT_PREFIX = "NebulostoreAsyncTestScript.";
  private static final String MAIN_PHASE_DURATION_ARGUMENT_NAME = ASYNC_TEST_SCRIPT_PREFIX +
      "main-phase-duration-minutes";
  private static final String SYNC_PHASE_DURATION_ARGUMENT_NAME = ASYNC_TEST_SCRIPT_PREFIX +
      "synchronization-phase-duration-minutes";

  private final int mainPhaseDurationMinutes_;
  private final int syncPhaseDurationMinutes_;
  private final String testId_;

  private final Map<CommAddress, Set<String>> messagesSentToPeers_ = new HashMap<>();
  private final Map<CommAddress, Set<String>> messagesReceivedByPeers_ = new HashMap<>();
  private final Map<CommAddress, List<Long>> messagesDelays_ = new HashMap<>();

  @Inject
  public NebulostoreAsyncTestScript(@Named(DATABASE_HOSTNAME_ARGUMENT_NAME) String hostname,
      @Named(DATABASE_PORT_ARGUMENT_NAME) String port,
      @Named(DATABASE_DATABASE_ARGUMENT_NAME) String database,
      @Named(DATABASE_USERNAME_ARGUMENT_NAME) String username,
      @Named(DATABASE_PASSWORD_ARGUMENT_NAME) String password,
      @Named(DATABASE_UPDATE_KEY_ARGUMENT_NAME) String canUpdate,
      @Named(FAILURE_PROBABILITY_ARGUMENT_NAME) double failureProbability,
      @Named(AVAIL_CHECKER_PERIOD_ARGUMENT_NAME) int availCheckerPeriodMilis,
      @Named(MAIN_PHASE_DURATION_ARGUMENT_NAME) int mainPhaseDurationMinutes,
      @Named(SYNC_PHASE_DURATION_ARGUMENT_NAME) int syncPhaseDurationMinutes,
      @Named(INIT_PHASE_DURATION_ARGUMENT_NAME) int initPhaseDurationMinutes,
      @Named(TEST_ID_ARGUMENT_NAME) String testId,
      EnvironmentPreparator<Environment> preparator) throws IOException {
    super(hostname, port, database, username, password, canUpdate, failureProbability,
        availCheckerPeriodMilis, initPhaseDurationMinutes, preparator);
    mainPhaseDurationMinutes_ = mainPhaseDurationMinutes;
    syncPhaseDurationMinutes_ = syncPhaseDurationMinutes;
    testId_ = testId;
  }

  @Override
  protected void runTest() {
    try {
      startFailureSimulation(new Function<NebulostoreApp, Void>() {

        @Override
        public Void apply(NebulostoreApp app) {
          LOGGER.debug("Starting messages sending, peer: " + app.getCommAddress());
          app.startAsyncHelperMessagesSending();
          LOGGER.debug("Started messages sending, peer: " + app.getCommAddress());
          return null;
        }

      });
      LOGGER.info("Starting main phase of the test.");
      TimeUnit.MINUTES.sleep(mainPhaseDurationMinutes_);
      LOGGER.info("Finished main phase of the test, preparing synchronization phase.");
      stopFailureSimulation();
      // Synchronization phase
      startApps(1.0, new Function<NebulostoreApp, Void>() {

        @Override
        public Void apply(NebulostoreApp app) {
          LOGGER.debug("Stopping messages sending, peer: " + app.getCommAddress());
          app.stopAsyncHelperMessagesSending();
          LOGGER.debug("Stopped messages sending, peer: " + app.getCommAddress());
          return null;
        }

      });
      LOGGER.info("Starting synchronization phase.");
      TimeUnit.MINUTES.sleep(syncPhaseDurationMinutes_);
      LOGGER.info("Synchronization phase finished, ending the test.");
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    gatherResults(messagesSentToPeers_, messagesReceivedByPeers_, messagesDelays_);
  }

  @Override
  protected TestResult calcResult() {
    cleanDatabase();
    return new TestResult(Type.SUCCESS, calcResultsString(messagesSentToPeers_,
      messagesReceivedByPeers_, messagesDelays_));
  }

  @Override
  protected int getTestDuration() {
    return mainPhaseDurationMinutes_;
  }

  /**
   * Gather test statistics and save them in the given maps.
   *
   * @param messagesSentToPeers
   * @param messagesReceivedByPeers
   */
  private void gatherResults(Map<CommAddress, Set<String>> messagesSentToPeers,
      Map<CommAddress, Set<String>> messagesReceivedByPeers,
      Map<CommAddress, List<Long>> messagesDelays) {
    for (NebulostoreApp app : apps_) {
      CommAddress appAddress = app.getCommAddress();
      Map<CommAddress, Set<String>> sentMessages = null;
      try {
        LOGGER.debug("Trying to get the sent messages directly from peer: " + appAddress);
        sentMessages = app.getSentAsyncTestMessages();
      } catch (IOException e) {
        // Peer is not responding, trying to get the result from database
        LOGGER.debug("Trying to get the sent messages from database for peer " + appAddress);
        String sentMessagesString =
            database_.get(AsyncTestHelperModule.ASYNC_HELPER_SENT_PREFIX + testId_ + appAddress);
        if (sentMessagesString != null) {
          sentMessages = new MessagesInfoDeserializer().apply(sentMessagesString);
        }
      }

      if (sentMessages != null) {
        for (Entry<CommAddress, Set<String>> entry : sentMessages.entrySet()) {
          if (!messagesSentToPeers.containsKey(entry.getKey())) {
            messagesSentToPeers.put(entry.getKey(), new HashSet<String>());
          }
          messagesSentToPeers.get(entry.getKey()).addAll(entry.getValue());
        }
      } else {
        LOGGER.warn("Could not get messages sent by peer " + appAddress);
      }

      Map<CommAddress, Set<String>> receivedMessages = null;
      try {
        LOGGER.debug("Trying to get the received messages directly from peer: " + appAddress);
        receivedMessages = app.getReceivedAsyncTestMessages();
      } catch (IOException e) {
        LOGGER.debug("Trying to get the received messages from database for peer " + appAddress);
        String receivedMsgsString = database_.get(
            AsyncTestHelperModule.ASYNC_HELPER_RECEIVED_PREFIX + testId_ + appAddress);
        if (receivedMsgsString != null) {
          receivedMessages = new MessagesInfoDeserializer().apply(receivedMsgsString);
        }
      }
      messagesReceivedByPeers.put(appAddress, new HashSet<String>());
      if (receivedMessages != null) {
        for (Set<String> msgIds : receivedMessages.values()) {
          messagesReceivedByPeers.get(appAddress).addAll(msgIds);
        }
      } else {
        LOGGER.warn("Could not get messages received by peer " + appAddress);
      }

      Map<String, Long> delays;
      try {
        LOGGER.debug("Trying to get the messages delays directly from peer: " + appAddress);
        delays = app.getMessagesDelays();
      } catch (IOException e) {
        LOGGER.debug("Trying to get the messages delays from database for peer " + appAddress);
        java.lang.reflect.Type type = (new TypeToken<Map<String, Long>>() { }).getType();
        delays =
            new Gson().fromJson(database_.get(
                AsyncTestHelperModule.ASYNC_HELPER_DELAYS_PREFIX + testId_ + appAddress), type);
      }

      if (delays == null) {
        LOGGER.warn("Could not get messages delays of peer " + appAddress);
      } else {
        messagesDelays.put(appAddress, Lists.newLinkedList(delays.values()));
      }
    }
  }

  private String calcResultsString(Map<CommAddress, Set<String>> messagesSentToPeers,
      Map<CommAddress, Set<String>> messagesReceivedByPeers,
      Map<CommAddress, List<Long>> messagesDelays) {
    int sentMessagesOverall = 0;
    int receivedMessagesOverall = 0;
    List<Long> overallDelays = new LinkedList<>();

    StringBuilder resultsBuilder = new StringBuilder();
    resultsBuilder.append("Test results:\n");
    for (NebulostoreApp app : apps_) {
      resultsBuilder.append("* Peer " + app.getCommAddress() + ":\n");
      CommAddress peer = app.getCommAddress();
      Set<String> sentMessages = messagesSentToPeers.get(peer);
      Set<String> receivedMessages = messagesReceivedByPeers.get(peer);
      if (sentMessages == null) {
        resultsBuilder.append("\tWARN: No information about messages sent to peer " + peer + "\n");
      } else if (receivedMessages == null) {
        resultsBuilder.append("\tWARN: No information about messages received by peer " + peer +
            "\n");
      } else {
        int sentSize = messagesSentToPeers.get(peer).size();
        int receivedSize = messagesReceivedByPeers.get(peer).size();
        appendStatisticsString(resultsBuilder, receivedSize, sentSize);
        resultsBuilder.append("\tLost messages' ids: " +
            Sets.difference(messagesSentToPeers.get(peer), messagesReceivedByPeers.get(peer)) +
            "\n");
        sentMessagesOverall += sentSize;
        receivedMessagesOverall += receivedSize;
      }

      if (messagesDelays.get(peer) == null) {
        resultsBuilder
            .append("\tWARN: No information about messages delays of peer " + peer + "\n");
      } else {
        resultsBuilder.append("\tAverage delay: ");
        List<Long> delays = messagesDelays.get(peer);
        overallDelays.addAll(delays);
        if (delays.size() == 0) {
          resultsBuilder.append(" No delays\n");
        } else {
          resultsBuilder.append(DoubleMath.mean(delays) + "\n");
        }
      }
    }
    resultsBuilder.append("Overall results: \n");
    appendStatisticsString(resultsBuilder, receivedMessagesOverall, sentMessagesOverall);
    resultsBuilder.append("\tAverage delay: ");
    if (overallDelays.size() == 0) {
      resultsBuilder.append(" No delays\n");
    } else {
      resultsBuilder.append(DoubleMath.mean(overallDelays) + "\n");
    }

    return resultsBuilder.toString();
  }

  private void cleanDatabase() {
    for (NebulostoreApp app : apps_) {
      CommAddress appAddress = app.getCommAddress();
      try {
        LOGGER.debug("Cleaning the database for peer " + appAddress);
        database_.delete(AsyncTestHelperModule.ASYNC_HELPER_SENT_PREFIX + testId_ + appAddress);
        database_.delete(AsyncTestHelperModule.ASYNC_HELPER_RECEIVED_PREFIX + testId_ + appAddress);
        database_.delete(AsyncTestHelperModule.ASYNC_HELPER_DELAYS_PREFIX + testId_ + appAddress);
        database_.delete(AsyncTestHelperModule.ASYNC_HELPER_LOGIN_COUNT_PREFIX +
            testId_ + appAddress);
        database_.delete(AsyncTestHelperModule.ASYNC_HELPER_LOGIN_MAP_PREFIX +
            testId_ + appAddress);
        LOGGER.debug("Cleaned the database for peer " + appAddress);
      } catch (IOException e) {
        LOGGER.warn("Error while trying to clean database entries for peer " + appAddress, e);
      }
    }
  }

  private void appendStatisticsString(StringBuilder builder, int receivedSize, int sentSize) {
    double percent = 0.0;
    if (sentSize != 0) {
      percent = 100.0 * (1.0 - ((double) receivedSize) / sentSize);
    }
    builder.append("\tNumber of messages sent to peer: " + sentSize + "\n");
    builder.append("\tNumber of messages received by peer: " + receivedSize + "\n");
    builder.append("\tLost messages percent: " + new DecimalFormat("#.##").format(percent) + "%\n");
  }

}
