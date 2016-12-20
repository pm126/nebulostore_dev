package org.nebulostore.dfuntest;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Doubles;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.bouncycastle.util.Arrays;
import org.junit.Test;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.FileChunk;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;

public class AvailabilityCalculationTester {

  private enum State { ENABLED, DISABLED };

  private static final int NUMBER_OF_PEERS = 100;
  private static final double FAILURE_PROBABILITY = 0.75;
  private static final double UNAVAILABILITY_FACTOR =
      FAILURE_PROBABILITY / (1.0 - FAILURE_PROBABILITY);
  private static final double AVAILABILITY_SHAPE = 1.9;
  private static final double AVAILABILITY_SCALE = 3.5;

  private static final long TEST_DURATION = 720;
  private static final long PERIOD_TIME_LIMIT = 1440;
  private static final long BASE_TEST_DURATION = 4 * 7 * PERIOD_TIME_LIMIT;
  private static final long TEST_PERIOD = 15;

  private static final int NUMBER_OF_ITERATIONS = 50;
  private static final int NUMBER_OF_TESTS = 5;

  private static final int SYNCHRO_PEERS_NUMBER = 5;
  private static final int MAX_RECIPIENTS_NUMBER = SYNCHRO_PEERS_NUMBER + 1;
  private static final double AVAILABILITY_TOLERANCE = 0.001;
  private static final int COMBINATION_LENGTH = 75;
  private static final int CHECK_PERIOD_SECONDS = 100;
  private static final int REQUIRED_PEERS = 15;

  private double unavailabilityScale_;
  private double unavailabilityShape_;

  private final Map<Integer, List<Long>> periods_ = new HashMap<>();
  private final Map<Integer, State> states_ = new HashMap<>();

  @Test
  public void sizeTest() throws CryptoException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, NoSuchPaddingException {
    byte[] data = new byte[1024];
    new Random().nextBytes(data);
    FileChunk chunk = new FileChunk(new NebuloAddress(new AppKey(BigInteger.ONE),
        new ObjectId(BigInteger.valueOf(806701))), 0);
    System.out.println("Data: " + data.length);
    System.out.println("Empty chunk: " + CryptoUtils.serializeObject(chunk).length);
    System.out.println(ArrayUtils.toString(CryptoUtils.serializeObject(chunk)));
    chunk.setData(data);
    System.out.println("Chunk with data: " + CryptoUtils.serializeObject(chunk).length);
    System.out.println(ArrayUtils.toString(CryptoUtils.serializeObject(chunk)));
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    PublicKey publicKey = keyGen.genKeyPair().getPublic();
    Cipher cipher = Cipher.getInstance("AES");
    KeyGenerator keyGen2 = KeyGenerator.getInstance("AES");
    keyGen2.init(128);
    SecretKey key = keyGen2.generateKey();
    cipher.init(Cipher.ENCRYPT_MODE, key);
    System.out.println(cipher.doFinal(CryptoUtils.serializeObject(chunk)).length);
    Cipher cipher2 = Cipher.getInstance("RSA");
    cipher2.init(Cipher.ENCRYPT_MODE, publicKey);
    cipher2.doFinal(CryptoUtils.serializeObject(key));
    System.out.println("What?");
    System.out.println(ArrayUtils.addAll(cipher2.doFinal(CryptoUtils.serializeObject(key)),
        cipher.doFinal(CryptoUtils.serializeObject(chunk))).length);
    //System.out.println(CryptoUtils.encryptObject(chunk, publicKey).getEncryptedData().length);

  }

  @Test
  public void codingTest() {
    paramAdjustmentTest();
    performSimulation();
    //calculateStatistics(true);
    //CodingTestRes res = codingTestRec(0, new HashSet<Integer>(), 0);
    int[] combination = new int[COMBINATION_LENGTH];
    for (int i = 0; i < COMBINATION_LENGTH; i++) {
      combination[i] = i;
    }
    int i = 0;
    int overallSuccCount = 0;
    int overallChecksCount = 0;
    do {
      long time = 1;
      System.out.println("Next combination: " + java.util.Arrays.toString(combination));
      int succCount = 0;
      int checksNumber = 0;
      while (time < TEST_DURATION * 60000) {
        //System.out.println("Time: " + time / 60000);
        int availCount = 0;
        for (Integer peer : combination) {
          if (isActive(peer, time)) {
            availCount++;
          }
        }
        if (availCount >= REQUIRED_PEERS) {
          //System.out.println("Success " + availCount);
          succCount++;
        } else {
          //System.out.println("Fail!");
        }
        checksNumber++;
        time += CHECK_PERIOD_SECONDS * 1000;
      }
      System.out.println("Succ count: " + succCount);
      System.out.println("Checks number: " + checksNumber);
      System.out.println("Probability: " + ((float) succCount) / checksNumber);
      overallSuccCount += succCount;
      overallChecksCount += checksNumber;

      int pos = new Random().nextInt(COMBINATION_LENGTH);
      int peer = new Random().nextInt(NUMBER_OF_PEERS);
      while (Arrays.contains(combination, peer)) {
        peer = new Random().nextInt(NUMBER_OF_PEERS);
      }
      combination[pos] = peer;
      i++;
    } while (i < 4000);
    System.out.println("OVERALL SUCC COUNT: " + overallSuccCount);
    System.out.println("OVERALL CHECKS COUNT: " + overallChecksCount);
    System.out.println("OVERALL PROBABILITY: " + ((float) overallSuccCount) /
        overallChecksCount);
    //commonParts();
  }

  private CodingTestRes codingTestRec(int startNumber, Set<Integer> combination, int count) {
    if (combination.size() == COMBINATION_LENGTH) {
      //PERFORM TEST
      long time = 1;
      System.out.println("Next combination: " + combination);
      int succCount = 0;
      int checksNumber = 0;
      while (time < TEST_DURATION * 60000) {
        //System.out.println("Time: " + time / 60000);
        int availCount = 0;
        for (Integer peer : combination) {
          if (isActive(peer, time)) {
            availCount++;
          }
        }
        if (availCount >= REQUIRED_PEERS) {
          //System.out.println("Success " + availCount);
          succCount++;
        } else {
          //System.out.println("Fail!");
        }
        checksNumber++;
        time += CHECK_PERIOD_SECONDS * 1000;
      }
      System.out.println("Succ count: " + succCount);
      System.out.println("Checks number: " + checksNumber);
      System.out.println("Probability: " + ((float) succCount) / checksNumber);
      return new CodingTestRes(succCount, checksNumber, count + 1);
    }

    CodingTestRes testRes = new CodingTestRes(0, 0, count);
    for (int i = startNumber; i < NUMBER_OF_PEERS; i++) {
      combination.add(i);
      CodingTestRes res = codingTestRec(i + 1, combination, testRes.count_);
      testRes.overallSuccCount_ += res.overallSuccCount_;
      testRes.overallChecksCont_ += res.overallChecksCont_;
      testRes.count_ = res.count_;
      if (testRes.count_ >= 8000) {
        return testRes;
      }
      combination.remove(i);
    }
    return testRes;
  }

  private boolean isActive(int peer, long time) {
    int pos = Collections.binarySearch(periods_.get(peer), time);
    if (pos < 0) {
      pos = 1 - pos;
    }
    return (periods_.get(peer).size() - pos) % 2 ==
        (states_.get(peer).equals(State.ENABLED) ? 1 : 0);
  }

  private void commonParts() {
    Map<Integer, State> initialStates = new HashMap<>();
    for (Entry<Integer, State> entry : states_.entrySet()) {
      if (periods_.get(entry.getKey()).size() % 2 == 1) {
        initialStates.put(entry.getKey(), State.values()[(states_.get(
            entry.getKey()).ordinal() + 1) % 2]);
      } else {
        initialStates.put(entry.getKey(), states_.get(entry.getKey()));
      }
    }

    long overallCommonPartLength = 0;
    long overallTime = 0;

    for (Entry<Integer, List<Long>> entry : periods_.entrySet()) {
      Integer peer = entry.getKey();
      List<Long> periods = entry.getValue();
      for (Entry<Integer, List<Long>> entry2 : periods_.entrySet()) {
        Integer peer2 = entry2.getKey();
        List<Long> periods2 = entry2.getValue();
        if (peer.equals(peer2)) {
          continue;
        }
        int i = 0;
        int j = 0;
        State state1 = initialStates.get(peer);
        State state2 = initialStates.get(peer2);
        long commonPartStart = 0;
        long commonPartLength = 0;
        while (i < periods.size() || j < periods2.size()) {
          if (i < periods.size() && (j == periods2.size() || periods.get(i) <= periods2.get(j))) {
            state1 = State.values()[(state1.ordinal() + 1) % 2];
            if (state1.equals(State.ENABLED) && state2.equals(State.ENABLED)) {
              commonPartStart = periods.get(i);
            } else if (state1.equals(State.DISABLED) && state2.equals(State.ENABLED)) {
              commonPartLength += periods.get(i) - commonPartStart;
            }
            i++;
          } else if (j < periods2.size() && (i == periods.size() ||
              periods.get(i) > periods2.get(j))) {
            state2 = State.values()[(state2.ordinal() + 1) % 2];
            if (state2.equals(State.ENABLED) && state1.equals(State.ENABLED)) {
              commonPartStart = periods2.get(j);
            } else if (state2.equals(State.DISABLED) && state1.equals(State.ENABLED)) {
              commonPartLength += periods2.get(j) - commonPartStart;
            }
            j++;
          }
        }
        overallCommonPartLength += commonPartLength;
        overallTime += periods.get(periods.size() - 1) - periods.get(0);
      }
    }

    System.out.println("PERCENTAGE OF COMMON PARTS: " + ((double) overallCommonPartLength) /
        overallTime);
  }

  private class CodingTestRes {
    public int overallSuccCount_;
    public int overallChecksCont_;
    public int count_;

    public CodingTestRes(int succCount, int checksCount, int count) {
      overallSuccCount_ = succCount;
      overallChecksCont_ = checksCount;
      count_ = count;
    }
  }

  @Test
  public void test() {
    paramAdjustmentTest();
    performSimulation();
    calculateStatistics(true);
  }

  @Test
  public void spAvailTest() {
    performSimulation();
    Map<Integer, State> states = new HashMap<>(states_);
    System.out.println("Overall availability percent: " + calculateStatistics(false) * 100 + " %");

    Map<Integer, HashSet<Integer>> synchroPeers = new HashMap<>();
    Map<Integer, Integer> recipientsNumbers = new HashMap<>();
    Map<Integer, State> initialStates = new HashMap<>();
    Random random = new Random();

    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      recipientsNumbers.put(i, 0);
      synchroPeers.put(i, new HashSet<Integer>());
      if (periods_.get(i).size() % 2 == 1) {
        initialStates.put(i, states.get(i));
      } else {
        initialStates.put(i, State.values()[(states.get(i).ordinal() + 1) % 2]);
      }
    }

    //System.out.println("Selecting synchro-peers");
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      for (int j = 0; j < SYNCHRO_PEERS_NUMBER; j++) {
        int synchroPeer = random.nextInt(NUMBER_OF_PEERS);
        while (synchroPeers.get(i).contains(synchroPeer) ||
            recipientsNumbers.get(synchroPeer) >= MAX_RECIPIENTS_NUMBER ||
            synchroPeer == i) {
          synchroPeer = random.nextInt(NUMBER_OF_PEERS);
        }

        synchroPeers.get(i).add(synchroPeer);
        recipientsNumbers.put(synchroPeer, recipientsNumbers.get(synchroPeer) + 1);
      }
    }
    //System.out.println("Synchro-peers selected: " + synchroPeers);


    long overallAvailabilityTime = 0;
    long overallTime = 0;
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      Map<Integer, List<Long>> periods = new HashMap<>();
      for (Entry<Integer, List<Long>> entry : periods_.entrySet()) {
        periods.put(entry.getKey(), new ArrayList<Long>(entry.getValue()));
      }
      Set<Integer> peersToCheck = new HashSet<Integer>(synchroPeers.get(i));
      peersToCheck.add(i);
      Set<Integer> activePeers = new HashSet<>();
      for (int j = 0; j < NUMBER_OF_PEERS; j++) {
        if (initialStates.get(j).equals(State.ENABLED)) {
          activePeers.add(j);
        }
      }

      long availabilityTime = 0;
      long wholeTime = 0;
      long currentTime = 0;

      Long nextPeriod = null;
      Long lastPeriod = null;
      do {
        nextPeriod = null;
        Integer minPeer = 0;
        for (int j = 0; j < NUMBER_OF_PEERS; j++) {
          Long period = null;
          if (!periods.get(j).isEmpty()) {
            period = periods.get(j).get(0);
          }

          if (nextPeriod == null || (period != null && period < nextPeriod)) {
            nextPeriod = period;
            minPeer = j;
          }
        }

        if (nextPeriod != null) {
          periods.get(minPeer).remove(0);

          if (lastPeriod != null) {
            wholeTime += nextPeriod - lastPeriod;
            currentTime += nextPeriod - lastPeriod;
          }

          lastPeriod = nextPeriod;
          if (activePeers.contains(minPeer)) {
            activePeers.remove(minPeer);
            if (Sets.intersection(activePeers, peersToCheck).isEmpty() &&
                peersToCheck.contains(minPeer)) {
              availabilityTime += currentTime;
            }
          } else {
            if (Sets.intersection(activePeers, peersToCheck).isEmpty() &&
                peersToCheck.contains(minPeer)) {
              currentTime = 0;
            }
            activePeers.add(minPeer);
          }

          if (peersToCheck.contains(minPeer)) {
//            System.out.println("Period: " + nextPeriod);
//            System.out.println(Sets.intersection(activePeers, peersToCheck));
          }
        }
      } while (nextPeriod != null);

      if (!Sets.intersection(activePeers, peersToCheck).isEmpty()) {
        availabilityTime += currentTime;
      }

      overallTime += wholeTime;
      overallAvailabilityTime += availabilityTime;
//      System.out.println("Availability time: " + availabilityTime);
//      System.out.println("Whole time: " + wholeTime);
//      System.out.println("Availability: " + ((double) availabilityTime) / wholeTime * 100 + "%");
    }


    System.out.println("Overall availability: " + ((double) overallAvailabilityTime) /
        overallTime * 100 + "%");
  }

  @Test
  public void multiTest() {
    for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
      System.out.println(i + ":");
      paramAdjustmentTest();
      periods_.clear();
      states_.clear();
      /*performSimulation();
      calculateStatistics(false);*/
      spAvailTest();

    }
  }

  @Test
  public void paramAdjustmentTest() {
    Double calculatedAvailability = null;
    double addition = 0.0;
    double lowerBound = -UNAVAILABILITY_FACTOR;
    double upperBound = 2 * UNAVAILABILITY_FACTOR;

    double targetAvailability = 1.0 - FAILURE_PROBABILITY;

    do {
      periods_.clear();
      states_.clear();

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
      }
      unavailabilityShape_ = calcUnavailabilityShape(UNAVAILABILITY_FACTOR + addition);
      unavailabilityScale_ = calcUnavailabilityScale(UNAVAILABILITY_FACTOR + addition);
      System.out.println("Starting simulation with addition set to: " + addition);
      double availabilitySum = 0.0;
      for (int i = 0; i < NUMBER_OF_TESTS; i++) {
        performSimulation();
        calculatedAvailability = calculateStatistics(false);
        availabilitySum += calculatedAvailability;
      }
      calculatedAvailability = availabilitySum / NUMBER_OF_TESTS;
      System.out.println("Result: " + calculatedAvailability);
    } while (Math.abs(calculatedAvailability - (1.0 - FAILURE_PROBABILITY)) >
      AVAILABILITY_TOLERANCE);

    System.out.println("Addition: " + addition);
    //System.out.println("Final parameters:");
    //System.out.println("Unavailability shape: \t" + unavailabilityShape_);
    //System.out.println("Unavailability scale: \t" + unavailabilityScale_);
  }

  /*
  @Test
  public void paramAdjTest2() {

    NebulostoreReliabilityTestScript testScript = mock(NebulostoreReliabilityTestScript.class);
    System.out.println("Avail scale: " + testScript.availabilityDistScale_);
    System.out.println("Avail shape: " + testScript.availabilityDistShape_);
    testScript.availCheckerPeriodMilis_ = (int) TEST_PERIOD;
    testScript.failureProbability_ = 0.75;
    when(testScript.getTestDuration()).thenReturn((int) TEST_DURATION);
    testScript.apps_ = new ArrayList<>();
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      testScript.apps_.add(mock(NebulostoreApp.class));
    }
    doCallRealMethod().when(testScript).prepareUnavailabilityDistParameters();
    doCallRealMethod().when(testScript).generateChangeTime(anyLong(), anyDouble(), anyDouble());
    doCallRealMethod().when(testScript).calcDistParameters(anyDouble());
    doCallRealMethod().when(testScript).performSimulation();
    testScript.prepareUnavailabilityDistParameters();
    System.out.println("Scale: " + testScript.unavailabilityDistScale_);
    System.out.println("Shape: " + testScript.unavailabilityDistShape_);
  }
  */

  private double calcUnavailabilityShape(double unavailabilityFactor) {
    return Math.sqrt(Math.log((Math.exp(Math.pow(AVAILABILITY_SHAPE, 2)) - 1) /
        Math.pow(unavailabilityFactor, 2) + 1));
  }

  private double calcUnavailabilityScale(double unavailabilityFactor) {
    return AVAILABILITY_SCALE + Math.log(unavailabilityFactor) + (Math.pow(AVAILABILITY_SHAPE, 2) -
        Math.pow(calcUnavailabilityShape(unavailabilityFactor), 2)) / 2;
  }

  private void switchState(int index) {
    if (states_.get(index).equals(State.ENABLED)) {
      states_.put(index, State.DISABLED);
    } else {
      states_.put(index, State.ENABLED);
    }
  }

  private long generateChangeTime(long lastTime, double mean, double shape) {
    RealDistribution distribution = new LogNormalDistribution(mean, shape);
    double period = distribution.sample();
    while (period > PERIOD_TIME_LIMIT) {
      period = distribution.sample();
    }

    if (period >= 3) {
      period = Math.max(period / BASE_TEST_DURATION * TEST_DURATION, 3);
    } else {
      period = period / BASE_TEST_DURATION * TEST_DURATION;
    }
    //System.out.println("period: " + period);
    return lastTime + Math.round(period * 60000);
  }

  private void performSimulation() {
    periods_.clear();
    states_.clear();
    long startTime = 0;
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      states_.put(i, State.ENABLED);
      if (new Random().nextDouble() < FAILURE_PROBABILITY) {
        states_.put(i, State.DISABLED);
        //System.out.println("Peer " + i + " DISABLED");
      } else {
        //System.out.println("Peer " + i + " ENABLED");
      }
      periods_.put(i, new ArrayList<Long>());
      periods_.get(i).add(startTime);
    }

    for (int peerIndex : states_.keySet()) {

      State state = states_.get(peerIndex);
      double factor = AVAILABILITY_SCALE;
      double shape = AVAILABILITY_SHAPE;
      long addition = 20 * 1000;
      if (state.equals(State.DISABLED)) {
        factor = unavailabilityScale_;
        shape = unavailabilityShape_;
        addition = 0;
      }
      long nextPeriodEndTime = generateChangeTime(
          periods_.get(peerIndex).get(periods_.get(peerIndex).size() - 1),
          factor, shape) + addition;
      periods_.get(peerIndex).add(nextPeriodEndTime);
    }

    long endTime = startTime + TEST_DURATION * 60000;

    long currTime = startTime;
    while (currTime < endTime) {
      for (int peerIndex : states_.keySet()) {
        long peerTime = periods_.get(peerIndex).get(periods_.get(peerIndex).size() - 1);
        if (currTime > peerTime) {
          State state = states_.get(peerIndex);
          double factor = AVAILABILITY_SCALE;
          double shape = AVAILABILITY_SHAPE;
          long addition = 0 * 1000;
          if (state.equals(State.ENABLED)) {
            factor = unavailabilityScale_;
            shape = unavailabilityShape_;
            addition = 0;
          }
          periods_.get(peerIndex).set(periods_.get(peerIndex).size() - 1, currTime);
          long nextPeriodEndTime = generateChangeTime(currTime, factor, shape) + addition;
          periods_.get(peerIndex).add(Math.min(nextPeriodEndTime, endTime));
          switchState(peerIndex);
        }
      }

      currTime += TEST_PERIOD * 1000;
    }
  }

  private double calculateStatistics(boolean debugMode) {
    long overallActivityTime = 0;
    long overallTime = 0;
    int longPeriods = 0;
    int periods = 0;
    List<Double> availabilityPeriods = new ArrayList<>();
    List<Double> unavailabilityPeriods = new ArrayList<>();
    if (debugMode) {
      System.out.println("Session and downtime times:");
    }
    for (int i = 0; i < NUMBER_OF_PEERS; i++) {
      if (debugMode) {
        //System.out.println("Peer " + i + ":");
      }
      if (periods_.get(i).size() % 2 == 1) {
        switchState(i);
      }

      long lastPeriod = periods_.get(i).get(0);
      long activityTime = 0;
      long inactivityTime = 0;
      long wholeTime = periods_.get(i).get(periods_.get(i).size() - 1) - periods_.get(i).get(0);

      overallTime += wholeTime;
      for (int j = 1; j < periods_.get(i).size(); j++) {
        double periodLength = (periods_.get(i).get(j) - lastPeriod) / 60000.0;
        //System.out.println("\t" + j + ". " + periodLength + " " + states_.get(i));
        if (states_.get(i).equals(State.DISABLED)) {
          unavailabilityPeriods.add(periodLength);
        } else {
          availabilityPeriods.add(periodLength);
        }
        if (states_.get(i).equals(State.ENABLED)) {
          activityTime += periods_.get(i).get(j) - lastPeriod;
          if (((periods_.get(i).get(j) - lastPeriod) / 60000.0) >= 30.0) {
            longPeriods++;
          }
          periods++;
        } else {
          inactivityTime += periods_.get(i).get(j) - lastPeriod;
        }
        //System.out.println("\t" + activityTime / 60000.0 + "\t" + inactivityTime / 60000.0);
        switchState(i);
        lastPeriod = periods_.get(i).get(j);
      }
      overallActivityTime += activityTime;

      if (debugMode) {
        System.out.println("\tActivity time: " + activityTime / 60000.0);
        System.out.println("\tWhole time: " + wholeTime / 60000.0);
        System.out.println("\tAvailability percent: " +
            ((double) activityTime) / wholeTime * 100 + "%");
      }

    }

    if (debugMode) {
      System.out.println("Overall results: ");
      System.out.println("\tActivity time: " + overallActivityTime / 60000.0);
      System.out.println("\tOverall time: " + overallTime / 60000.0);
      System.out.println("\tAvailability percent: " +
          ((double) overallActivityTime) / overallTime * 100 + "%");
    }

    if (debugMode) {
      System.out.println("Periods: " + longPeriods + "/" + periods);
    }

    Ordering<Double> ordering = new Ordering<Double>() {

      @Override
      public int compare(Double left, Double right) {
        return Doubles.compare(left, right);
      }
    };

    if (debugMode) {
      System.out.println("Min length of availability period: " + ordering.min(availabilityPeriods));
      System.out.println("Max length of availability period: " + ordering.max(availabilityPeriods));
      System.out.println("Mean value of availability periods' length: " +
          DoubleMath.mean(availabilityPeriods));
      StandardDeviation deviation = new StandardDeviation();
      double[] primAvailArray = ArrayUtils.
          toPrimitive(availabilityPeriods.toArray(new Double[availabilityPeriods.size()]));
      System.out.println("Sd of availability periods' lengths: " +
          deviation.evaluate(primAvailArray));
      Median median = new Median();
      System.out.println("Median of availability periods' lengths: " +
          median.evaluate(primAvailArray));

      System.out.println("Min length of unavailability period: " +
          ordering.min(unavailabilityPeriods));
      System.out.println("Max length of unavailability period: " +
          ordering.max(unavailabilityPeriods));
      System.out.println("Mean value of unavailability periods' length: " +
          DoubleMath.mean(unavailabilityPeriods));
      double[] primUnavailArray = ArrayUtils.
          toPrimitive(unavailabilityPeriods.toArray(new Double[unavailabilityPeriods.size()]));
      System.out.println("Sd of unavailability periods' lengths: " +
          deviation.evaluate(primUnavailArray));
      System.out.println("Median of unavailability periods' lengths: " +
          median.evaluate(primUnavailArray));
    }

    return ((double) overallActivityTime) / overallTime;
  }

}
