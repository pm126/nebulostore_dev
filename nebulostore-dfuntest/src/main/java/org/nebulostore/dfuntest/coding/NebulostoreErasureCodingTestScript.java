package org.nebulostore.dfuntest.coding;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.NebulostoreApp;
import org.nebulostore.dfuntest.NebulostoreReliabilityTestScript;
import org.nebulostore.dfuntest.coding.CodingTestHelperModule.CodingTestResults;

public class NebulostoreErasureCodingTestScript extends NebulostoreReliabilityTestScript {

  private static final Logger LOGGER = Logger.getLogger(NebulostoreErasureCodingTestScript.class);

  private static final String CODING_TEST_PREFIX = "NebulostoreErasureCodingTestScript.";

  private static final String STORE_PHASE_DURATION_ARGUMENT_NAME = CODING_TEST_PREFIX +
      "store-phase-duration-minutes";
  private static final String MAIN_PHASE_DURATION_ARGUMENT_NAME = CODING_TEST_PREFIX +
      "main-phase-duration-minutes";
  private static final String PEER_START_PHASE_DURATION_ARGUMENT_NAME = CODING_TEST_PREFIX +
      "peer-start-phase-duration-minutes";

  private final int storePhaseDurationMinutes_;
  private final int mainPhaseDurationMinutes_;
  private final int peerStartPhaseDurationMinutes_;
  private final String testId_;
  private final Map<CommAddress, CodingTestResults> testResults_ = new HashMap<>();

  @Inject
  public NebulostoreErasureCodingTestScript(
      @Named(DATABASE_HOSTNAME_ARGUMENT_NAME) String hostname,
      @Named(DATABASE_PORT_ARGUMENT_NAME) String port,
      @Named(DATABASE_DATABASE_ARGUMENT_NAME) String database,
      @Named(DATABASE_USERNAME_ARGUMENT_NAME) String username,
      @Named(DATABASE_PASSWORD_ARGUMENT_NAME) String password,
      @Named(DATABASE_UPDATE_KEY_ARGUMENT_NAME) String canUpdate,
      @Named(FAILURE_PROBABILITY_ARGUMENT_NAME) double failureProbability,
      @Named(AVAIL_CHECKER_PERIOD_ARGUMENT_NAME) int availCheckerPeriodMilis,
      @Named(INIT_PHASE_DURATION_ARGUMENT_NAME) int initPhaseDurationMinutes,
      @Named(STORE_PHASE_DURATION_ARGUMENT_NAME) int storePhaseDurationMinutes,
      @Named(MAIN_PHASE_DURATION_ARGUMENT_NAME) int mainPhaseDurationMinutes,
      @Named(PEER_START_PHASE_DURATION_ARGUMENT_NAME) int peerStartPhaseDurationMinutes,
      @Named(TEST_ID_ARGUMENT_NAME) String testId,
      EnvironmentPreparator<Environment> preparator) throws IOException {
    super(hostname, port, database, username, password, canUpdate, failureProbability,
        availCheckerPeriodMilis, initPhaseDurationMinutes, preparator);
    storePhaseDurationMinutes_ = storePhaseDurationMinutes;
    mainPhaseDurationMinutes_ = mainPhaseDurationMinutes;
    peerStartPhaseDurationMinutes_ = peerStartPhaseDurationMinutes;
    testId_ = testId;
  }

  @Override
  protected void runTest() {
    try {
      storeObjects();
      TimeUnit.MINUTES.sleep(storePhaseDurationMinutes_);
      startFailureSimulation(null);
      LOGGER.info("Starting main phase of the test");
      LOGGER.debug("Applying function for reader");
      apps_.get(0).startReaderMode();
      TimeUnit.MINUTES.sleep(mainPhaseDurationMinutes_);
      apps_.get(0).stopReaderMode();
      stopFailureSimulation();
      startApps(1.0, null);
      LOGGER.info("Starting synchronization phase of the test");
      TimeUnit.MINUTES.sleep(peerStartPhaseDurationMinutes_);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    collectResults(testResults_);
  }

  @Override
  protected TestResult calcResult() {
    try {
      cleanDatabase();
    } catch (IOException e) {
      LOGGER.warn("Error while cleaning the database.", e);
    }
    return new TestResult(Type.SUCCESS, calcResultsString(testResults_));
  }

  @Override
  protected int getTestDuration() {
    return mainPhaseDurationMinutes_;
  }

  private void storeObjects() {
    ExecutorService executor = Executors.newCachedThreadPool();
    NebulostoreApp zeroApp = apps_.get(0);
    for (final NebulostoreApp app : apps_) {
      if (!app.equals(zeroApp)) {
        LOGGER.debug("Storing objects, peer: " + app.getCommAddress());
        app.storeObjects();
        LOGGER.debug("Stored objects, peer: " + app.getCommAddress());
      }
    }
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void collectResults(Map<CommAddress, CodingTestResults> testResults) {
    for (NebulostoreApp app : apps_) {
      CodingTestResults results = null;
      String testResultsString = null;
      try {
        LOGGER.debug("Trying to get test results directly from peer " + app.getCommAddress());
        testResultsString = app.getCodingTestResults();
        if (testResultsString != null) {
          results = new Gson().fromJson(testResultsString, CodingTestResults.class);
        }
      } catch (IOException e) {
        LOGGER.debug("Does not work!", e);
        // Peer is not responding, trying to get the result from database
        LOGGER.debug("Trying to get test results from database for peer " + app.getCommAddress());
        testResultsString = database_.get(CodingTestHelperModule.CODING_TEST_RESULTS_KEY + testId_ +
            app.getCommAddress());
        if (testResultsString != null) {
          results = new Gson().fromJson(testResultsString, CodingTestResults.class);
        }
      }

      if (results == null) {
        LOGGER.warn("Could not get test results from reader");
      } else {
        testResults.put(app.getCommAddress(), results);
      }
    }
  }

  private String calcResultsString(Map<CommAddress, CodingTestResults> testResults) {
    StringBuilder builder = new StringBuilder();
    builder.append(calcFileReadResultsString(testResults));
    builder.append(calcSizeAndTimeResultsString(testResults));
    return builder.toString();
  }

  private String calcFileReadResultsString(Map<CommAddress, CodingTestResults> testResults) {
    NebulostoreApp zeroApp = apps_.get(0);
    StringBuilder builder = new StringBuilder();
    CodingTestResults results = testResults.get(zeroApp.getCommAddress());
    if (results != null) {
      int successfulReads = 0;
      for (int val : results.successfulOperations_.values()) {
        successfulReads += val;
      }
      int unsuccessfulReads = 0;
      for (int val : results.unsuccessfulOperations_.values()) {
        unsuccessfulReads += val;
      }
      builder
          .append("\tNumber of read operations: " + (successfulReads + unsuccessfulReads) + "\n");
      builder.append("\tNumber of successful read operations: " + successfulReads + "\n");
      builder.append("\tSuccessful reads percent: " +
          new DecimalFormat("#.##").format(((float) successfulReads) /
          (successfulReads + unsuccessfulReads) * 100) + "%\n");
    }
    return builder.toString();
  }

  private String calcSizeAndTimeResultsString(Map<CommAddress, CodingTestResults> testResults) {
    StringBuilder builder = new StringBuilder();

    List<Double> overallReceivedDataSizes = new LinkedList<>();
    List<Long> overallRecoveryDurations = new LinkedList<>();
    List<Double> overallSentDataSizes = new LinkedList<>();
    for (NebulostoreApp app : Iterables.skip(apps_, 1)) {
      CommAddress appAddress = app.getCommAddress();
      builder.append("Peer ").append(appAddress).append(":\n");
      CodingTestResults results = testResults.get(appAddress);

      builder.append(calcTimeAndSizeResultsSingle(results.receivedDataSizes_,
          results.recoveryDurations_, results.sentDataSizes_));
      if (results.receivedDataSizes_ != null) {
        overallReceivedDataSizes.addAll(results.receivedDataSizes_);
      }

      if (results.recoveryDurations_ != null) {
        overallRecoveryDurations.addAll(results.recoveryDurations_);
      }

      if (results.sentDataSizes_ != null) {
        overallSentDataSizes.addAll(results.sentDataSizes_);
      }
    }

    builder.append("Overall results:\n").append(calcTimeAndSizeResultsSingle(
        overallReceivedDataSizes, overallRecoveryDurations, overallSentDataSizes));

    return builder.toString();
  }

  private String calcTimeAndSizeResultsSingle(List<Double> receivedDataSizes,
      List<Long> recoveryDurations, List<Double> sentDataSizes) {
    StringBuilder builder = new StringBuilder();

    builder.append("\tNumber of received data examples: ");
    if (receivedDataSizes == null) {
      builder.append("0\n");
    } else {
      builder.append(receivedDataSizes.size()).append("\n");
    }
    builder.append("\tAverage size of received data: ");
    if (receivedDataSizes == null || receivedDataSizes.isEmpty()) {
      builder.append("No info\n");
    } else {
      builder.append(new DecimalFormat("#.##").format(DoubleMath.mean(receivedDataSizes))).
        append(" Bytes\n");
      builder.append("\tNumber of zero examples: " +
          Collections.frequency(receivedDataSizes, 0.0) + "\n");
    }
    //FIXME to też można byłoby jakoś uogólnić, bo kod się powtarza...
    builder.append("\n\tNumber of sent data examples: ");
    if (sentDataSizes == null) {
      builder.append("0\n");
    } else {
      builder.append(sentDataSizes.size()).append("\n");
    }
    builder.append("\tAverage size of sent data: ");
    if (sentDataSizes == null || sentDataSizes.isEmpty()) {
      builder.append("No info\n");
    } else {
      builder.append(new DecimalFormat("#.##").format(DoubleMath.mean(sentDataSizes))).
        append(" Bytes\n");
    }

    builder.append("\tAverage recovery time: ");
    if (recoveryDurations == null || recoveryDurations.isEmpty()) {
      builder.append("No info\n");
    } else {
      builder.append(new DecimalFormat("#.##").format(DoubleMath
          .mean(recoveryDurations))).append(" miliseconds\n");
    }

    return builder.toString();
  }

  private void cleanDatabase() throws IOException {
    for (NebulostoreApp app : apps_) {
      LOGGER.debug("Cleaning database entry for peer " + app.getCommAddress());
      database_.delete(CodingTestHelperModule.CODING_TEST_RESULTS_KEY + testId_ +
          app.getCommAddress());
    }
  }

}
