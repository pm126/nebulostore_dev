package org.nebulostore.dfuntest.comm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.NebulostoreApp;
import org.nebulostore.dfuntest.NebulostoreReliabilityTestScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulostoreCommPerfTestScript extends NebulostoreReliabilityTestScript {

  private static final Logger LOGGER = LoggerFactory.getLogger(NebulostoreCommPerfTestScript.class);

  private static final String COMM_TEST_SCRIPT_PREFIX = "NebulostoreCommPerfTestScript.";
  private static final String MAIN_PHASE_DURATION_ARGUMENT_NAME = COMM_TEST_SCRIPT_PREFIX +
      "main-phase-duration-minutes";

  private final int mainPhaseDurationMinutes_;

  private final Map<CommAddress, Set<String>> messagesSentToPeers_ = new HashMap<>();
  private final Map<CommAddress, Set<String>> messagesReceivedByPeers_ = new HashMap<>();

  @Inject
  public NebulostoreCommPerfTestScript(@Named(DATABASE_HOSTNAME_ARGUMENT_NAME) String hostname,
      @Named(DATABASE_PORT_ARGUMENT_NAME) String port,
      @Named(DATABASE_DATABASE_ARGUMENT_NAME) String database,
      @Named(DATABASE_USERNAME_ARGUMENT_NAME) String username,
      @Named(DATABASE_PASSWORD_ARGUMENT_NAME) String password,
      @Named(DATABASE_UPDATE_KEY_ARGUMENT_NAME) String canUpdate,
      @Named(FAILURE_PROBABILITY_ARGUMENT_NAME) double failureProbability,
      @Named(AVAIL_CHECKER_PERIOD_ARGUMENT_NAME) int availCheckerPeriodMilis,
      @Named(MAIN_PHASE_DURATION_ARGUMENT_NAME) int mainPhaseDurationMinutes,
      @Named(INIT_PHASE_DURATION_ARGUMENT_NAME) int initPhaseDurationMinutes,
      EnvironmentPreparator<Environment> preparator) throws IOException {
    super(hostname, port, database, username, password, canUpdate, failureProbability,
        availCheckerPeriodMilis, initPhaseDurationMinutes, preparator);
    mainPhaseDurationMinutes_ = mainPhaseDurationMinutes;
  }

  @Override
  protected void runTest() {
    try {
      TimeUnit.MINUTES.sleep(mainPhaseDurationMinutes_);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ExecutorService executor = Executors.newCachedThreadPool();
    for (final NebulostoreApp app : apps_) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          LOGGER.debug("Stopping CommPerfHelper messages sending at peer: " + app.getCommAddress());
          app.stopCommPerfHelperMessagesSending();
          LOGGER.debug("Stopped CommPerfHelper messages sending at peer: " + app.getCommAddress());
        }

      });
    }
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    try {
      TimeUnit.MINUTES.sleep(3);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    gatherResults(messagesSentToPeers_, messagesReceivedByPeers_);
  }

  @Override
  protected TestResult calcResult() {
    int sentMessagesOverall = 0;
    int receivedMessagesOverall = 0;

    StringBuilder resultsBuilder = new StringBuilder();
    resultsBuilder.append("Test results:\n");
    for (NebulostoreApp app : apps_) {
      resultsBuilder.append("* Peer " + app.getCommAddress() + ":\n");
      CommAddress peer = app.getCommAddress();
      Set<String> sentMessages = messagesSentToPeers_.get(peer);
      Set<String> receivedMessages = messagesReceivedByPeers_.get(peer);
      if (sentMessages == null) {
        resultsBuilder.append("\tWARN: No information about messages sent to peer " + peer + "\n");
      } else if (receivedMessages == null) {
        resultsBuilder.append("\tWARN: No information about messages received by peer " + peer +
            "\n");
      } else {
        int sentSize = messagesSentToPeers_.get(peer).size();
        int receivedSize = messagesReceivedByPeers_.get(peer).size();
        appendStatisticsString(resultsBuilder, receivedSize, sentSize);
        sentMessagesOverall += sentSize;
        receivedMessagesOverall += receivedSize;
      }
    }
    resultsBuilder.append("Overall results: \n");
    appendStatisticsString(resultsBuilder, receivedMessagesOverall, sentMessagesOverall);

    return new TestResult(Type.SUCCESS, resultsBuilder.toString());
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

  @Override
  protected int getTestDuration() {
    return mainPhaseDurationMinutes_;
  }

  private void gatherResults(Map<CommAddress, Set<String>> messagesSentToPeers,
      Map<CommAddress, Set<String>> messagesReceivedByPeers) {
    for (NebulostoreApp app : apps_) {
      CommAddress appAddress = app.getCommAddress();
      Map<CommAddress, Set<String>> sentMessages = null;
      try {
        LOGGER.debug("Trying to get the sent messages directly from peer: " + appAddress);
        sentMessages = app.getSentCommPerfTestMessages();
      } catch (IOException e) {
        LOGGER.debug("Could not get the sent messages from peer: " + appAddress);
      }

      if (sentMessages != null) {
        for (Entry<CommAddress, Set<String>> entry : sentMessages.entrySet()) {
          if (!messagesSentToPeers.containsKey(entry.getKey())) {
            messagesSentToPeers.put(entry.getKey(), new HashSet<String>());
          }
          messagesSentToPeers.get(entry.getKey()).addAll(entry.getValue());
        }
      }

      Map<CommAddress, Set<String>> receivedMessages = null;
      try {
        LOGGER.debug("Trying to get the received messages directly from peer: " + appAddress);
        receivedMessages = app.getReceivedCommPerfTestMessages();
      } catch (IOException e) {
        LOGGER.debug("Could not get the received messages from peer: " + appAddress);
      }

      messagesReceivedByPeers.put(appAddress, new HashSet<String>());
      if (receivedMessages != null) {
        for (Set<String> msgIds : receivedMessages.values()) {
          messagesReceivedByPeers.get(appAddress).addAll(msgIds);
        }
      }
    }
  }

}
