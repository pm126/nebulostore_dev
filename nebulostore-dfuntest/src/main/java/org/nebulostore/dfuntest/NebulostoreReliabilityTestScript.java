package org.nebulostore.dfuntest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import me.gregorias.dfuntest.CommandException;
import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;
import me.gregorias.dfuntest.TestScript;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.persistence.SQLKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test script for asynchronous messages performance test. It starts given nebulostore apps and
 * waits the specified time. During the test all peers send messages to all known peers
 * periodically. After waiting time scripts disables messages sending in each application and waits
 * some time to give peers the possibility to receive messages that were sent recently. Next it
 * gathers information about received and sent messages directly from each application or from the
 * test database if the application is inaccessible. Using this information the test scripts
 * generates statistics which are the final result of this test.
 *
 * @author Piotr Malicki
 *
 */
public abstract class NebulostoreReliabilityTestScript implements TestScript<NebulostoreApp> {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(NebulostoreReliabilityTestScript.class);

  protected static final String DATABASE_ARGUMENTS_PREFIX = "test-database.";
  protected static final String DATABASE_HOSTNAME_ARGUMENT_NAME = DATABASE_ARGUMENTS_PREFIX +
      "host";
  protected static final String DATABASE_PORT_ARGUMENT_NAME = DATABASE_ARGUMENTS_PREFIX + "port";
  protected static final String DATABASE_DATABASE_ARGUMENT_NAME = DATABASE_ARGUMENTS_PREFIX +
      "database";
  protected static final String DATABASE_USERNAME_ARGUMENT_NAME = DATABASE_ARGUMENTS_PREFIX +
      "user";
  protected static final String DATABASE_PASSWORD_ARGUMENT_NAME = DATABASE_ARGUMENTS_PREFIX +
      "password";
  protected static final String DATABASE_UPDATE_KEY_ARGUMENT_NAME = DATABASE_ARGUMENTS_PREFIX +
      "update_key";
  protected static final String RELIABILITY_TEST_PREFIX = "NebulostoreReliabilityTestScript.";
  protected static final String FAILURE_PROBABILITY_ARGUMENT_NAME = RELIABILITY_TEST_PREFIX +
      "failure-probability";
  protected static final String AVAIL_CHECKER_PERIOD_ARGUMENT_NAME = RELIABILITY_TEST_PREFIX +
      "avail-checker-period-milis";
  protected static final String INIT_PHASE_DURATION_ARGUMENT_NAME = RELIABILITY_TEST_PREFIX +
      "init-phase-duration-minutes";
  protected static final String TEST_ID_ARGUMENT_NAME = "dfuntest.test-id";

  protected static final int MAX_THREADS_NUMBER = 30;
  protected static final double PERIOD_LENGTH_LIMIT = 24 * 60;
  protected static final double BASE_TEST_DURATION = 4 * 7 * PERIOD_LENGTH_LIMIT;
  protected static final double MIN_SESSION_LENGTH_LIMIT = 3.0;

  private static final double AVAILABILITY_TOLERANCE = 0.0001;
  private static final double NUMBER_OF_SIMULATION_ITERS = 5;
  private static final int APP_START_WAIT_MILIS = 20 * 1000;
  private static final int APP_AVG_START_TIME_MILIS = 1600;
  private static final int APP_SHUTDOWN_DURATION_MILIS = 30 * 1000;

  private enum AppState {
    AVAILABLE, UNAVAILABLE
  }

  public List<NebulostoreApp> apps_ = new ArrayList<>();
  private final Set<NebulostoreApp> startedApps_ = Sets.newConcurrentHashSet();
  private final Map<NebulostoreApp, Long> changeTimes_ = new HashMap<>();
  private final Map<NebulostoreApp, AppState> appStates_ = new HashMap<>();
  protected ScheduledExecutorService executor_;

  protected final KeyValueStore<String> database_;
  private final EnvironmentPreparator<Environment> preparator_;
  private final double failureProbability_;
  private final double availabilityDistScale_ = 3.5;
  private final double availabilityDistShape_ = 1.9;
  private double unavailabilityDistScale_;
  private double unavailabilityDistShape_;
  private final int availCheckerPeriodMilis_;
  private final int initPhaseDurationMinutes_;

  @Inject
  public NebulostoreReliabilityTestScript(@Named(DATABASE_HOSTNAME_ARGUMENT_NAME) String hostname,
      @Named(DATABASE_PORT_ARGUMENT_NAME) String port,
      @Named(DATABASE_DATABASE_ARGUMENT_NAME) String database,
      @Named(DATABASE_USERNAME_ARGUMENT_NAME) String username,
      @Named(DATABASE_PASSWORD_ARGUMENT_NAME) String password,
      @Named(DATABASE_UPDATE_KEY_ARGUMENT_NAME) String canUpdate,
      @Named(FAILURE_PROBABILITY_ARGUMENT_NAME) double failureProbability,
      @Named(AVAIL_CHECKER_PERIOD_ARGUMENT_NAME) int availCheckerPeriodMilis,
      @Named(INIT_PHASE_DURATION_ARGUMENT_NAME) int initPhaseDurationMinutes,
      EnvironmentPreparator<Environment> preparator) throws IOException {
    database_ =
        new SQLKeyValueStore<String>(hostname, port, database, username, password,
            Boolean.parseBoolean(canUpdate), new Function<String, byte[]>() {
              @Override
              public byte[] apply(String input) {
                return input.getBytes();
              }
            }, new Function<byte[], String>() {
              @Override
              public String apply(byte[] input) {
                return new String(input);
              }
            });
    failureProbability_ = failureProbability;
    availCheckerPeriodMilis_ = availCheckerPeriodMilis;
    initPhaseDurationMinutes_ = initPhaseDurationMinutes;
    preparator_ = preparator;
  }

  @Override
  public TestResult run(final Collection<NebulostoreApp> apps) {
    // Sort apps based on their ids
    apps_.addAll(apps);
    executor_ = Executors.newScheduledThreadPool(apps_.size());
    sortAppsByIds(apps_);
    prepareUnavailabilityDistParameters();

    if (!startApps(1.0, null)) {
      return new TestResult(Type.FAILURE, "Could not start up application.");
    }

    try {
      TimeUnit.MINUTES.sleep(initPhaseDurationMinutes_);
    } catch (InterruptedException e) {
      // FIXME
      e.printStackTrace();
    }

    runTest();
    shutDownApps();

    return calcResult();
  }

  protected abstract void runTest();

  protected abstract TestResult calcResult();

  protected boolean startApps(double probability,
    final Function<NebulostoreApp, Void> postStartAction) {
    ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS_NUMBER);
    NebulostoreApp zeroApp = apps_.get(0);
    if (!zeroApp.isRunning()) {
      try {
        zeroApp.startUp();
        startedApps_.add(zeroApp);
        try {
          TimeUnit.MINUTES.sleep(3);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      } catch (CommandException | IOException e) {
        LOGGER.error("Zero application could not be started!", e);
        return false;
      }
    }

    if (postStartAction != null) {
      postStartAction.apply(zeroApp);
    }

    Random random = new Random();
    long changeTime = 0;
    for (int i = 1; i < apps_.size(); i++) {
      final NebulostoreApp app = apps_.get(i);
      if (random.nextDouble() < probability) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            if (!app.isRunning()) {
              try {
                app.startUp();
                startedApps_.add(app);
              } catch (CommandException | IOException e) {
                LOGGER.warn("Peer " + app.getCommAddress() + " could not be started.");
                return;
              }
            }

            try {
              TimeUnit.MILLISECONDS.sleep(APP_START_WAIT_MILIS);
            } catch (InterruptedException e) {
              LOGGER.warn("Interrupted while waiting for peer " + app.getCommAddress() +
                  " to start");
            }

            if (postStartAction != null) {
              postStartAction.apply(app);
            }
          }
        });
        if (probability < 1.0) {
          changeTime =
              generateChangeTime(System.currentTimeMillis(), availabilityDistScale_,
                  availabilityDistShape_);
          appStates_.put(app, AppState.AVAILABLE);
        }
      } else {
        if (probability < 1.0) {
          changeTime =
              generateChangeTime(System.currentTimeMillis(), unavailabilityDistScale_,
                  unavailabilityDistShape_);
          appStates_.put(app, AppState.UNAVAILABLE);
        }
        executor.execute(new Runnable() {
          @Override
          public void run() {
            if (app.isRunning()) {
              try {
                app.shutDown();
                startedApps_.remove(app);
              } catch (IOException e) {
                LOGGER.warn("Peer " + app.getCommAddress() + " could not be stopped.");
              }
            }
          }
        });
      }
      changeTimes_.put(app, changeTime);
    }
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return true;
  }

  protected void startFailureSimulation(Function<NebulostoreApp, Void> postStartAction) {
    startApps(1.0 - failureProbability_, postStartAction);
    for (NebulostoreApp app : apps_) {
      if (!app.getCommAddress().equals(CommAddress.getZero())) {
        executor_.scheduleWithFixedDelay(new AvailabilityCheckerService(app, postStartAction),
            availCheckerPeriodMilis_, availCheckerPeriodMilis_, TimeUnit.MILLISECONDS);
      }
    }
  }

  protected void stopFailureSimulation() throws InterruptedException {
    executor_.shutdown();
    LOGGER.debug("Executor ended: " + executor_.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS));
  }

  /**
   * Returns duration of the test phase during which the peers are switched on and off.
   *
   */
  protected abstract int getTestDuration();

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
      }
      calcDistParameters(unavailabilityFactor + addition);
      LOGGER.debug("Calculated dist parameters for " + unavailabilityFactor + " and addition: " +
          addition);
      LOGGER.debug("New unavailability scale: " + unavailabilityDistScale_);
      LOGGER.debug("New unavailability shape: " + unavailabilityDistShape_);
      double availabilitySum = 0.0;
      for (int i = 0; i < NUMBER_OF_SIMULATION_ITERS; i++) {
        calculatedAvailability = performSimulation();
        availabilitySum += calculatedAvailability;
      }
      calculatedAvailability = availabilitySum / NUMBER_OF_SIMULATION_ITERS;
      LOGGER.debug("Calculated availability: " + calculatedAvailability + " with addition: " +
          addition);
    } while (Math.abs(calculatedAvailability - (1.0 - failureProbability_)) >
      AVAILABILITY_TOLERANCE);
  }

  private void calcDistParameters(double unavailabilityFactor) {
    unavailabilityDistShape_ =
        Math.sqrt(Math.log((Math.exp(Math.pow(availabilityDistShape_, 2)) - 1) /
            Math.pow(unavailabilityFactor, 2) + 1));
    unavailabilityDistScale_ =
        availabilityDistScale_ + Math.log(unavailabilityFactor) +
            (Math.pow(availabilityDistShape_, 2) - Math.pow(unavailabilityDistShape_, 2)) / 2;
  }

  private void shutDownApps() {
    ExecutorService shutDownExecutor = Executors.newFixedThreadPool(MAX_THREADS_NUMBER);
    List<NebulostoreApp> startedAppsList = Lists.newArrayList(startedApps_);
    sortAppsByIds(startedAppsList);
    for (final NebulostoreApp app : Lists.reverse(startedAppsList)) {
      shutDownExecutor.submit(new Runnable() {

        @Override
        public void run() {
          try {
            app.shutDown();
          } catch (IOException e) {
            LOGGER.warn("Peer " + app.getCommAddress() + " could not be stopped.");
          }
        }
      });
    }
    try {
      shutDownExecutor.shutdown();
      shutDownExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void sortAppsByIds(List<NebulostoreApp> apps) {
    Collections.sort(apps, new Comparator<NebulostoreApp>() {
      @Override
      public int compare(NebulostoreApp app1, NebulostoreApp app2) {
        return Integer.compare(app1.getId(), app2.getId());
      }
    });
  }

  private long generateChangeTime(long startTime, double scale, double shape) {
    RealDistribution distribution = new LogNormalDistribution(scale, shape);
    double period = distribution.sample();
    while (period > PERIOD_LENGTH_LIMIT) {
      period = distribution.sample();
    }

    if (period >= MIN_SESSION_LENGTH_LIMIT) {
      period = Math.max(period / BASE_TEST_DURATION * getTestDuration(), MIN_SESSION_LENGTH_LIMIT);
    } else {
      period = period / BASE_TEST_DURATION * getTestDuration();
    }
    LOGGER.debug("Period: " + period);
    return startTime + Math.round(period * 60000.0);

  }

  // FIXME to by można wydzielić do osobnej klasy
  private double performSimulation() {
    Map<Integer, Long> lastPeriods = new HashMap<>();
    Map<Integer, AppState> states = new HashMap<>();
    Map<Integer, Long> uptimeSums = new HashMap<>();

    long startTime = 0;
    long endTime = startTime + getTestDuration() * 60000;
    for (int i = 0; i < apps_.size(); i++) {
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
            currTime += APP_START_WAIT_MILIS;
            nextChangeTime = generateChangeTime(currTime, availabilityDistScale_,
                availabilityDistShape_);
            uptimeSums.put(peerIndex, uptimeSums.get(peerIndex) +
                Math.min(nextChangeTime, endTime) - currTime +
                APP_START_WAIT_MILIS - APP_AVG_START_TIME_MILIS);
          } else {
            uptimeSums.put(peerIndex, uptimeSums.get(peerIndex) +
                currTime - lastPeriods.get(peerIndex));
            nextChangeTime = generateChangeTime(currTime,
                unavailabilityDistScale_, unavailabilityDistShape_);
            currTime += APP_SHUTDOWN_DURATION_MILIS;
          }
          lastPeriods.put(peerIndex, nextChangeTime);
          states
              .put(peerIndex, AppState.values()[(state.ordinal() + 1) % AppState.values().length]);
        }
        currTime += availCheckerPeriodMilis_;
      }
    }


    long overallAvailabilityTime = 0;
    for (long availabilityTime : uptimeSums.values()) {
      overallAvailabilityTime += availabilityTime;
    }

    long overallTime = getTestDuration() * apps_.size() * 60000L;
    return overallTime == 0 ? 1.0 : (double) overallAvailabilityTime / overallTime;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  private class AvailabilityCheckerService implements Runnable {
    private final NebulostoreApp app_;
    private final Function<NebulostoreApp, Void> postStartAction_;

    public AvailabilityCheckerService(NebulostoreApp app,
        Function<NebulostoreApp, Void> postStartAction) {
      postStartAction_ = postStartAction;
      app_ = app;
    }

    @Override
    public void run() {
      boolean isRunning = app_.isRunning();
      if (appStates_.get(app_).equals(AppState.UNAVAILABLE)) {
        try {
          if (!isRunning) {
            if (changeTimes_.containsKey(app_) &&
                changeTimes_.get(app_) < System.currentTimeMillis()) {
              app_.tryPrepareEnvironment(preparator_);
              app_.startUp();
            } else {
              return;
            }
          } else if (!changeTimes_.containsKey(app_) ||
              changeTimes_.get(app_) > System.currentTimeMillis()) {
            app_.shutDown();
            return;
          }
          // FIXME dwie następne linijki się dublują
          startedApps_.add(app_);
          appStates_.put(app_, AppState.AVAILABLE);
          try {
            TimeUnit.MILLISECONDS.sleep(APP_START_WAIT_MILIS);
          } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for peer " + app_.getCommAddress() +
                " to start");
          }
          changeTimes_.put(
              app_,
              generateChangeTime(System.currentTimeMillis(), availabilityDistScale_,
                  availabilityDistShape_));

          if (postStartAction_ != null) {
            postStartAction_.apply(app_);
          }


        } catch (CommandException | IOException e) {
          LOGGER.info("Peer " + app_.getCommAddress() + " could not be started", e);
        }
      } else {
        try {
          if (isRunning) {
            if (changeTimes_.containsKey(app_) &&
                changeTimes_.get(app_) < System.currentTimeMillis()) {
              startedApps_.remove(app_);
              appStates_.put(app_, AppState.UNAVAILABLE);
              changeTimes_.put(
                  app_,
                  generateChangeTime(System.currentTimeMillis(), unavailabilityDistScale_,
                      unavailabilityDistShape_));
              app_.shutDown();
            } else {
              return;
            }
          } else if (!changeTimes_.containsKey(app_) ||
              changeTimes_.get(app_) > System.currentTimeMillis()) {
            app_.tryPrepareEnvironment(preparator_);
            app_.startUp();
            try {
              TimeUnit.MILLISECONDS.sleep(APP_START_WAIT_MILIS);
            } catch (InterruptedException e) {
              LOGGER.warn("Interrupted while waiting for peer " + app_.getCommAddress() +
                  " to start");
            }
            if (postStartAction_ != null) {
              postStartAction_.apply(app_);
            }
            return;
          }
        } catch (IOException | CommandException e) {
          LOGGER.info("Peer " + app_.getCommAddress() + " could not be stopped", e);
        }
      }
    }
  }
}
