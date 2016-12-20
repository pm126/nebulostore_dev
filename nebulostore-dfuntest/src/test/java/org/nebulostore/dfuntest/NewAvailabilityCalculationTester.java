package org.nebulostore.dfuntest;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;

public class NewAvailabilityCalculationTester {

  protected static final int MAX_THREADS_NUMBER = 30;
  protected static final double PERIOD_LENGTH_LIMIT = 24 * 60;
  protected static final double BASE_TEST_DURATION = 64 * 4 * 7 * PERIOD_LENGTH_LIMIT;
  protected static final double MIN_SESSION_LENGTH_LIMIT = 3.0;

  private static final double AVAILABILITY_TOLERANCE = 0.001;
  private static final double NUMBER_OF_SIMULATION_ITERS = 1;
  private static final int NUMBER_OF_PEERS = 45;

  private static final long TEST_DURATION = (long) BASE_TEST_DURATION;// 12 * 60;
  // (long) BASE_TEST_DURATION;
  private static final long FIRST_WAIT_DURATION = 3;
  private static final long INIT_PHASE_DURATION = 1;
  private static final long AVAIL_CHECKER_PERIOD_MILIS = 15000;

  private static final int IN_SYMBOLS_NUMBER = 7;
  private static final int OUT_SYMBOLS_NUMBER = 42;
  private static final int NUMBER_OF_COMBINATIONS = 100;
  private static final int APP_START_TIME_DURATION = 20 * 1000;
  private static final int APP_SHUTDOWN_TIME_DURATION = 30 * 1000;
  private static final int NUMBER_OF_TESTS = 50;

  private enum AppState {
    AVAILABLE, UNAVAILABLE
  }

  protected ScheduledExecutorService executor_;

  private final double failureProbability_ = 0.75;
  private final double availabilityDistScale_ = 3.5;
  private final double availabilityDistShape_ = 1.9;
  private double unavailabilityDistScale_;
  private double unavailabilityDistShape_;

  private long numberOfMinPeriods_;
  private long numberOfPeriods_;

  @Test
  public void codingAvailabilityTest() {
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    final DecimalFormat format = new DecimalFormat("#.00", otherSymbols);

    DescriptiveStatistics availabilityStats = new DescriptiveStatistics();
    DescriptiveStatistics differencesStats = new DescriptiveStatistics();
    DescriptiveStatistics stDevStats = new DescriptiveStatistics();
    DescriptiveStatistics minAvStats = new DescriptiveStatistics();
    DescriptiveStatistics maxAvStats = new DescriptiveStatistics();

    for (int k = 0; k < NUMBER_OF_TESTS; k++) {
      prepareUnavailabilityDistParameters();

      final Map<Integer, List<Long>> periodsMap = new HashMap<>();
      Map<Integer, AppState> stateMap = new HashMap<>();
      for (int i = 0; i < NUMBER_OF_PEERS; i++) {
        periodsMap.put(i, new ArrayList<Long>());
        stateMap.put(i, AppState.UNAVAILABLE);
      }
      long startTime = 0;
      long currentTime = startTime;
      periodsMap.get(0).add(currentTime);
      currentTime += FIRST_WAIT_DURATION * 60000;
      switchState(stateMap, 0);
      for (int i = 1; i < NUMBER_OF_PEERS; i++) {
        switchState(stateMap, i);
        periodsMap.get(i).add(currentTime);
      }

      currentTime += INIT_PHASE_DURATION * 60000;
      final long testStart = currentTime;

      final long endTime = currentTime + TEST_DURATION * 60000;
      periodsMap.get(0).add(endTime);
      for (int i = 1; i < NUMBER_OF_PEERS; i++) {
        long currTime = testStart;

        Random random = new Random();
        if (random.nextDouble() < 1 - failureProbability_) {
          periodsMap.get(i).add(
              generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_, true));
        } else {
          periodsMap.get(i).add(currTime);
          periodsMap.get(i)
              .add(
                  generateChangeTime(currTime, unavailabilityDistScale_, unavailabilityDistShape_,
                      true));
          stateMap.put(i, AppState.UNAVAILABLE);
          currTime += APP_SHUTDOWN_TIME_DURATION;
        }

        currTime += AVAIL_CHECKER_PERIOD_MILIS;

        while (currTime < endTime) {
          if (periodsMap.get(i).get(periodsMap.get(i).size() - 1) <= currTime) {
            List<Long> periods = periodsMap.get(i);
            if (stateMap.get(i).equals(AppState.AVAILABLE)) {
              periods.set(periods.size() - 1, currTime);
              periodsMap.get(i).add(
                  Math.min(
                      endTime,
                      generateChangeTime(currTime, unavailabilityDistScale_,
                          unavailabilityDistShape_, true)));
              currTime += APP_SHUTDOWN_TIME_DURATION;
            } else {
              currTime += 1600;
              periods.set(periods.size() - 1, currTime);
              currTime += APP_START_TIME_DURATION - 1600;
              periodsMap.get(i).add(
                  Math.min(
                      endTime,
                      generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_,
                          true)));
            }
            switchState(stateMap, i);
          }
          currTime += AVAIL_CHECKER_PERIOD_MILIS;
        }
      }

      Map<Integer, AppState> tempStatesMap = new HashMap<>();
      Map<Integer, Long> workTimes = new HashMap<>();
      long overallWorkTime = 0;
      DescriptiveStatistics stats = new DescriptiveStatistics();
      for (int i = 1; i < NUMBER_OF_PEERS; i++) {
        int pos = Collections.binarySearch(periodsMap.get(i), testStart);
        if (pos > 0) {
          tempStatesMap.put(i, AppState.values()[(pos + 1) % 2]);
        } else {
          tempStatesMap.put(i, AppState.values()[-pos % 2]);
          pos = -pos - 1;
        }

        // pos is set to the first period equal to or higher than testStart
        List<Long> periods = periodsMap.get(i);
        long workTime = 0;
        long lastPeriod = testStart;
        for (int j = pos; j < periods.size(); j++) {
          if (tempStatesMap.get(i).equals(AppState.AVAILABLE)) {
            workTime += periods.get(j) - lastPeriod;
          }
          lastPeriod = periods.get(j);
          switchState(tempStatesMap, i);
        }
        System.out.println("Peer " + i + " worktime: " + ((double) workTime) /
            (endTime - testStart));
        stats.addValue(((double) workTime) / (endTime - testStart));
        overallWorkTime += workTime;
      }
      double avgAvailability =
          ((double) overallWorkTime) / ((endTime - testStart) * (NUMBER_OF_PEERS - 1));

      availabilityStats.addValue(avgAvailability * 100);
      stDevStats.addValue(stats.getStandardDeviation());
      minAvStats.addValue(stats.getMin());
      maxAvStats.addValue(stats.getMax());

      System.out.println("Overall work time: " + format.format(avgAvailability * 100) + "%");

      System.out.println("Mean: " + stats.getMean());
      System.out.println("Standard deviation: " +
          new DecimalFormat("#.####", otherSymbols).format(stats.getStandardDeviation()));
      System.out.println("Min: " + format.format(stats.getMin() * 100) + "%");
      System.out.println("Max: " + format.format(stats.getMax() * 100) + "%");

      double theorAvailability = 0.0;
      for (int i = IN_SYMBOLS_NUMBER; i <= OUT_SYMBOLS_NUMBER; i++) {
        double tmpAvailability = 1.0;
        for (int j = 1; j <= i; j++) {
          tmpAvailability *= ((double) (OUT_SYMBOLS_NUMBER - j + 1)) / j;
        }
        tmpAvailability *= Math.pow(avgAvailability, i);
        tmpAvailability *= Math.pow(1.0 - avgAvailability, OUT_SYMBOLS_NUMBER - i);
        theorAvailability += tmpAvailability;
      }
      System.out.println("Theoretical object availability: " +
          format.format(theorAvailability * 100) + "%");

      /*
       * System.out.println("Periods: "); boolean isActive = false; for (Long period :
       * periodsMap.get(1)) { isActive = !isActive; System.out.println(period); if (isActive) {
       * System.out.println("v"); } else { System.out.println("x"); } }
       */

      final ConcurrentLinkedQueue<Double> results = new ConcurrentLinkedQueue<>();

      System.out.println("Object availability: ");
      ExecutorService executor = Executors.newFixedThreadPool(4);
      for (int i = 0; i < 1; i++) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            double result = calcCodingAvailability(testStart, endTime, periodsMap, format);
            results.add(result);
            System.out.println("Average availability: " + format.format(result * 100) + "%");
          }
        });
      }

      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      double meanResult = 0.0;
      for (Double result : results) {
        meanResult += result;
      }
      meanResult = meanResult / results.size();

      differencesStats.addValue((meanResult - theorAvailability) * 100);

      /*
       * System.out.println("Real:"); for (int i = 0; i < 1; i++) {
       * calcRealCodingAvailability(testStart, endTime, periodsMap); }
       */
    }

    System.out.println("=========================== Overall results: ==========================");
    System.out.println("Mean difference:\t" + differencesStats.getMean());
    System.out.println("St. dev. of differences:\t" + differencesStats.getStandardDeviation());
    System.out.println("Min difference:\t" + differencesStats.getMin());
    System.out.println("Max difference:\t" + differencesStats.getMax());
    System.out.println("Mean availability:\t" + availabilityStats.getMean());
    System.out.println("St. dev. of availabilities:\t" + availabilityStats.getStandardDeviation());
    System.out.println("Min availability:\t" + availabilityStats.getMin());
    System.out.println("Max availability:\t" + availabilityStats.getMax());

    System.out.println("Percentage of minimal sessions: " + ((double) numberOfMinPeriods_) /
        numberOfPeriods_ * 100 + "%");
  }

  public void calcRealCodingAvailability(long testStart, long endTime,
    Map<Integer, List<Long>> periodsMap) {
    List<Integer> peers = new LinkedList<>();
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      peers.add(i);
    }

    long testTime = testStart;
    int readsCount = 0;
    int succCount = 0;
    while (testTime < endTime) {
      Collections.shuffle(peers);
      int successfulReads = 0;
      for (int j = 0; j < OUT_SYMBOLS_NUMBER; j++) {
        int peer = peers.get(j);
        int pos = Math.abs(Collections.binarySearch(periodsMap.get(peer), testTime) + 1);
        if (pos % 2 == 1) {
          successfulReads += 1;
        }
      }
      if (successfulReads >= IN_SYMBOLS_NUMBER) {
        succCount++;
      }
      readsCount++;
      testTime += 3000;
    }

    System.out.println("Average availability: " + ((double) succCount) / readsCount);
  }

  public double calcCodingAvailability(long testStart, long endTime,
    Map<Integer, List<Long>> periodsMap, DecimalFormat format) {
    List<Integer> peers = new LinkedList<>();
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      peers.add(i);
    }

    int overallReadsCount = 0;
    int overallSuccCount = 0;
    for (int i = 0; i < NUMBER_OF_COMBINATIONS; i++) {
      Collections.shuffle(peers);
      long testTime = testStart;
      int readsCount = 0;
      int succCount = 0;
      // System.out.println("Period: " + getTestDuration() * 60000L / 14400L);
      while (testTime < endTime) {
        int successfulReads = 0;
        for (int j = 0; j < OUT_SYMBOLS_NUMBER; j++) {
          int peer = peers.get(j);
          int pos = Math.abs(Collections.binarySearch(periodsMap.get(peer), testTime) + 1);
          if (pos % 2 == 1) {
            // System.out.println("Successful read at time: " + testTime);
            successfulReads += 1;
          } else {
            // System.out.println("Failed read at time: " + testTime);
          }
        }
        if (successfulReads >= IN_SYMBOLS_NUMBER) {
          succCount++;
        }
        readsCount++;
        testTime += getTestDuration() * 60000L / 14400L;
      }
      overallSuccCount += succCount;
      overallReadsCount += readsCount;
    }
    // System.out.println(overallReadsCount);
    return ((double) overallSuccCount) / overallReadsCount;
  }

  @Test
  public void repeatedSimulationTest() {
    prepareUnavailabilityDistParameters();
    double sum = 0.0;
    for (int i = 0; i < 30; i++) {
      System.out.println(i + ":");
      sum += simulationTest();
    }

    System.out.println("Simulation average result: " + sum / 30.0);
  }

  @Test
  public void objectAvailabilityTest() {
    prepareUnavailabilityDistParameters();

    Map<Integer, List<Long>> periodsMap = new HashMap<>();
    Map<Integer, AppState> stateMap = new HashMap<>();
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      periodsMap.put(i, new ArrayList<Long>());
      stateMap.put(i, AppState.UNAVAILABLE);
    }
    long startTime = 0;
    long currentTime = startTime;
    periodsMap.get(0).add(currentTime);
    currentTime += FIRST_WAIT_DURATION * 60000;
    switchState(stateMap, 0);
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      switchState(stateMap, i);
      periodsMap.get(i).add(currentTime);
    }

    currentTime += INIT_PHASE_DURATION * 60000;
    long testStart = currentTime;

    long endTime = currentTime + TEST_DURATION * 60000;
    periodsMap.get(0).add(endTime);
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      long currTime = testStart;

      Random random = new Random();
      if (random.nextDouble() < 1 - failureProbability_) {
        periodsMap.get(i).add(
            generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_, true));
      } else {
        periodsMap.get(i).add(currTime);
        periodsMap.get(i).add(
            generateChangeTime(currTime, unavailabilityDistScale_, unavailabilityDistShape_, true));
        stateMap.put(i, AppState.UNAVAILABLE);
        currTime += APP_SHUTDOWN_TIME_DURATION;
      }

      currTime += AVAIL_CHECKER_PERIOD_MILIS;

      while (currTime < endTime) {
        if (periodsMap.get(i).get(periodsMap.get(i).size() - 1) <= currTime) {
          List<Long> periods = periodsMap.get(i);
          if (stateMap.get(i).equals(AppState.AVAILABLE)) {
            periods.set(periods.size() - 1, currTime);
            periodsMap.get(i).add(
                Math.min(
                    endTime,
                    generateChangeTime(currTime, unavailabilityDistScale_,
                        unavailabilityDistShape_, true)));
            currTime += APP_SHUTDOWN_TIME_DURATION;
          } else {
            currTime += 2408;
            periods.set(periods.size() - 1, currTime);
            currTime += 3600;
            periodsMap.get(i).add(
                Math.min(
                    endTime,
                    generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_,
                        true)));
          }
          switchState(stateMap, i);
        }
        currTime += AVAIL_CHECKER_PERIOD_MILIS;
      }
    }

    // Select random peer combinations
    List<Integer> peers = new LinkedList<>();
    Collections.shuffle(peers);

  }

  public double simulationTest() {
    Map<Integer, List<Long>> periodsMap = new HashMap<>();
    Map<Integer, AppState> stateMap = new HashMap<>();
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      periodsMap.put(i, new ArrayList<Long>());
      stateMap.put(i, AppState.UNAVAILABLE);
    }
    long startTime = 0;
    long currentTime = startTime;
    periodsMap.get(0).add(currentTime);
    currentTime += FIRST_WAIT_DURATION * 60000;
    switchState(stateMap, 0);
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      switchState(stateMap, i);
      periodsMap.get(i).add(currentTime);

    }

    currentTime += INIT_PHASE_DURATION * 60000;
    long testStart = currentTime;

    long endTime = currentTime + TEST_DURATION * 60000;
    periodsMap.get(0).add(endTime);
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      long currTime = testStart;

      Random random = new Random();
      if (random.nextDouble() < 1 - failureProbability_) {
        periodsMap.get(i).add(
            generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_, true));
      } else {
        periodsMap.get(i).add(currTime);
        periodsMap.get(i).add(
            generateChangeTime(currTime, unavailabilityDistScale_, unavailabilityDistShape_, true));
        stateMap.put(i, AppState.UNAVAILABLE);
        currTime += APP_SHUTDOWN_TIME_DURATION;
      }

      currTime += AVAIL_CHECKER_PERIOD_MILIS;

      while (currTime < endTime) {
        if (periodsMap.get(i).get(periodsMap.get(i).size() - 1) <= currTime) {
          List<Long> periods = periodsMap.get(i);
          if (stateMap.get(i).equals(AppState.AVAILABLE)) {
            periods.set(periods.size() - 1, currTime);
            periodsMap.get(i).add(
                Math.min(
                    endTime,
                    generateChangeTime(currTime, unavailabilityDistScale_,
                        unavailabilityDistShape_, true)));
            currTime += APP_SHUTDOWN_TIME_DURATION;
          } else {
            currTime += 2408;
            periods.set(periods.size() - 1, currTime);
            currTime += 3600;
            periodsMap.get(i).add(
                Math.min(
                    endTime,
                    generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_,
                        true)));
          }
          switchState(stateMap, i);
        }
        currTime += AVAIL_CHECKER_PERIOD_MILIS;
      }
    }
    // System.out.println(periodsMap);

    Map<Integer, AppState> tempStatesMap = new HashMap<>();
    Map<Integer, Long> workTimes = new HashMap<>();
    long overallWorkTime = 0;
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      // System.out.println("Peer " + i + ":");
      // System.out.print("c(");
      for (Long period : periodsMap.get(i)) {
        // System.out.print((period - testStart) + ",");
      }
      // System.out.println(")");
      int pos = Collections.binarySearch(periodsMap.get(i), testStart);
      if (pos > 0) {
        tempStatesMap.put(i, AppState.values()[(pos + 1) % 2]);
      } else {
        tempStatesMap.put(i, AppState.values()[-pos % 2]);
        pos = -pos - 1;
      }

      // pos is set to the first period equal to or higher than testStart
      List<Long> periods = periodsMap.get(i);
      long workTime = 0;
      long lastPeriod = testStart;
      for (int j = pos; j < periods.size(); j++) {
        if (tempStatesMap.get(i).equals(AppState.AVAILABLE)) {
          workTime += periods.get(j) - lastPeriod;
        }
        lastPeriod = periods.get(j);
        switchState(tempStatesMap, i);
      }
      // System.out.println("Availability time: " + workTime);
      // System.out.println("Overall time: " + (endTime - testStart));
      // System.out.println("Peer " + i + " worktime: " + ((double) workTime) / (endTime -
      // testStart));
      overallWorkTime += workTime;
    }
    System.out.println("Overall work time: " + ((double) overallWorkTime) /
        ((endTime - testStart) * (NUMBER_OF_PEERS - 1)));

    return ((double) overallWorkTime) / ((endTime - testStart) * (NUMBER_OF_PEERS - 1));
  }

  private void switchState(Map<Integer, AppState> stateMap, int i) {
    stateMap.put(i, AppState.values()[(stateMap.get(i).ordinal() + 1) % AppState.values().length]);
  }

  /**
   * Returns duration of the test phase during which the peers are switched on and off.
   *
   */
  protected long getTestDuration() {
    return TEST_DURATION;
  }

  private void prepareUnavailabilityDistParameters() {
    Double calculatedAvailability = null;
    double unavailabilityFactor = failureProbability_ / (1.0 - failureProbability_);
    double addition = 0.0;
    double lowerBound = -unavailabilityFactor;
    double upperBound = 5 * unavailabilityFactor;

    double targetAvailability = 1.0 - failureProbability_;
    do {
      if (calculatedAvailability != null) {
        if (calculatedAvailability > targetAvailability) {
          if (addition > lowerBound) {
            lowerBound = addition;
          }
          addition = (upperBound + addition) / 2;
        } else {
          if (addition < upperBound) {
            upperBound = addition;
          }
          addition = (lowerBound + addition) / 2;
        }
        System.out.println("New addition: " + addition);
      }
      calcDistParameters(unavailabilityFactor + addition);
      double availabilitySum = 0.0;
      DescriptiveStatistics stats = new DescriptiveStatistics();
      for (int i = 0; i < NUMBER_OF_SIMULATION_ITERS; i++) {
        calculatedAvailability = performSimulation();
        availabilitySum += calculatedAvailability;
        stats.addValue(calculatedAvailability);
      }
      calculatedAvailability = availabilitySum / NUMBER_OF_SIMULATION_ITERS;
      System.out.println("Calculated availability: " + calculatedAvailability + "with addition: " +
          addition);
      System.out.println("Mean: " + stats.getMean());
      System.out.println("Standard deviation: " + stats.getStandardDeviation());
      System.out.println("Min: " + stats.getMin());
      System.out.println("Max: " + stats.getMax());
    } while (Math.abs(calculatedAvailability - (1.0 - failureProbability_)) > AVAILABILITY_TOLERANCE);
    System.out.println("Calculated addition: " + addition);
    System.out.println("Test runs: ");
    double sum = 0.0;
    for (int i = 0; i < 10; i++) {
      sum += performSimulation();
    }
    System.out.println(sum / 10);
  }

  private void calcDistParameters(double unavailabilityFactor) {
    unavailabilityDistShape_ =
        Math.sqrt(Math.log((Math.exp(Math.pow(availabilityDistShape_, 2)) - 1) /
            Math.pow(unavailabilityFactor, 2) + 1));
    unavailabilityDistScale_ =
        availabilityDistScale_ + Math.log(unavailabilityFactor) +
            (Math.pow(availabilityDistShape_, 2) - Math.pow(unavailabilityDistShape_, 2)) / 2;
  }

  private long generateChangeTime(long startTime, double scale, double shape, boolean count) {
    RealDistribution distribution = new LogNormalDistribution(scale, shape);
    double period = distribution.sample();
    while (period > PERIOD_LENGTH_LIMIT) {
      period = distribution.sample();
    }

    if (period >= MIN_SESSION_LENGTH_LIMIT) {
      if (count) {
        if (period / BASE_TEST_DURATION * getTestDuration() < MIN_SESSION_LENGTH_LIMIT) {
          numberOfMinPeriods_++;
        }
        numberOfPeriods_++;
      }

      period = Math.max(period / BASE_TEST_DURATION * getTestDuration(), MIN_SESSION_LENGTH_LIMIT);
    } else {
      period = period / BASE_TEST_DURATION * getTestDuration();
    }
    return startTime + Math.round(period * 60000.0);
    // return startTime + (scale == availabilityDistScale_ ? 40000 : 3600000);

  }

  private double performSimulation() {
    Map<Integer, Long> lastPeriods = new HashMap<>();
    Map<Integer, AppState> states = new HashMap<>();
    Map<Integer, Long> uptimeSums = new HashMap<>();

    long startTime = 0;
    long endTime = startTime + getTestDuration() * 60000L;
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      states.put(i, AppState.UNAVAILABLE);
      if (new Random().nextDouble() < failureProbability_) {
        states.put(i, AppState.AVAILABLE);
      }
      lastPeriods.put(i, startTime);
      uptimeSums.put(i, 0L);
    }

    for (int peerIndex : states.keySet()) {
      long currTime = startTime;
      while (currTime < endTime) {
        long peerTime = lastPeriods.get(peerIndex);
        if (currTime >= peerTime) {
          AppState state = states.get(peerIndex);
          long nextChangeTime = 0L;
          if (state.equals(AppState.UNAVAILABLE)) {
            currTime += APP_START_TIME_DURATION;
            uptimeSums.put(peerIndex, uptimeSums.get(peerIndex) + APP_START_TIME_DURATION - 1600);
            nextChangeTime =
                generateChangeTime(currTime, availabilityDistScale_, availabilityDistShape_, false);
            uptimeSums.put(peerIndex, uptimeSums.get(peerIndex) +
                Math.min(nextChangeTime, endTime) - currTime);
          } else {
            uptimeSums.put(peerIndex,
                uptimeSums.get(peerIndex) + currTime - lastPeriods.get(peerIndex));
            nextChangeTime =
                generateChangeTime(currTime, unavailabilityDistScale_, unavailabilityDistShape_,
                    false);
            currTime += APP_SHUTDOWN_TIME_DURATION;
          }
          lastPeriods.put(peerIndex, nextChangeTime);
          states
              .put(peerIndex, AppState.values()[(state.ordinal() + 1) % AppState.values().length]);
        }
        currTime += AVAIL_CHECKER_PERIOD_MILIS;
      }
      // System.out.println("Availability time: " + uptimeSums.get(peerIndex));
      // System.out.println(((double) uptimeSums.get(peerIndex)) / (getTestDuration() * 60000L));
    }

    long overallAvailabilityTime = 0;
    for (long availabilityTime : uptimeSums.values()) {
      overallAvailabilityTime += availabilityTime;
    }

    long overallTime = getTestDuration() * NUMBER_OF_PEERS * 60000L;
    return overallTime == 0 ? 1.0 : (double) overallAvailabilityTime / overallTime;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
