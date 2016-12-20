package org.nebulostore.dfuntest.coding;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.coding.messages.RecreateObjectFragmentsStatisticsMessage;
import org.nebulostore.dfuntest.coding.messages.RecreationSentDataSizeMessage;
import org.nebulostore.dfuntest.coding.messages.TestNebuloObjectAddressesMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.persistence.FileStore;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.timer.Timer;

/**
 * Helper module for erasure coding tests.
 *
 * @author Piotr Malicki
 *
 */
public class CodingTestHelperModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(CodingTestHelperModule.class);
  private static final int NUMBER_OF_OBJECTS = 1;
  private static final int DATA_LENGTH = 5 * 1024 * 1024;
  private static final String CODING_TEST_DATA_KEY = "CodingTestData";
  public static final String CODING_TEST_RESULTS_KEY = "CodingTestResults";
  private static final int CHECK_PERIOD_MILIS = 1 * 10 * 1000;

  private KeyValueStore<List<NebuloFile>> store_;
  private final NebuloObjectFactory factory_;
  private final Timer timer_;
  private final KeyValueStore<CodingTestResults> externalStore_;
  private final CommAddress myAddress_;
  private final String testId_;
  private final NebuloObjectFactory nebuloObjectFactory_;
  private final MessageVisitor visitor_ = new CodingTestHelperModuleVisitor();

  private final List<NebuloFile> objects_ = new LinkedList<>();
  private final List<NebuloAddress> objectAddresses_ = new LinkedList<>();
  private final Map<ObjectId, Integer> successfulOperations_ = new HashMap<>();
  private final Map<ObjectId, Integer> unsuccessfulOperations_ = new HashMap<>();
  private final List<Double> receivedDataSizes_ = new LinkedList<>();
  private final List<Long> recoveryDurations_ = new LinkedList<>();
  private final List<Double> sentDataSizes_ = new LinkedList<>();
  private Mode mode_ = Mode.NONE;

  private enum Mode {
    NONE, PASSIVE, READER
  };

  @Inject
  public CodingTestHelperModule(NebuloObjectFactory factory, Timer timer,
      @Named("CodingTestHelperStore") KeyValueStore<CodingTestResults> externalStore,
      CommAddress myAddress, @Named("test-id") String testId,
      NebuloObjectFactory nebuloObjectFactory) throws IOException {
    factory_ = factory;
    timer_ = timer;
    externalStore_ = externalStore;
    myAddress_ = myAddress;
    testId_ = testId;
    nebuloObjectFactory_ = nebuloObjectFactory;
    store_ =
        new FileStore<List<NebuloFile>>("codingTest",
            new Function<List<NebuloFile>, byte[]>() {
            @Override
            public byte[] apply(List<NebuloFile> objects) {
              return SerializationUtils.serialize((Serializable) objects);
            }
          }, new Function<byte[], List<NebuloFile>>() {
              @SuppressWarnings("unchecked")
              @Override
              public List<NebuloFile> apply(byte[] objects) {
                return (List<NebuloFile>) SerializationUtils.deserialize(objects);
              }
            });
  }

  public void storeObjects() {
    logger_.debug("Storing objects");
    mode_ = Mode.PASSIVE;
    for (int i = 0; i < NUMBER_OF_OBJECTS; i++) {
      logger_.debug("Storing next test object");
      NebuloFile file = factory_.createNewNebuloFile();
      logger_.debug("File created: " + file);
      byte[] data = new byte[DATA_LENGTH];
      new Random().nextBytes(data);
      logger_.debug(data.length);
      try {
        file.write(data, 0);
        logger_.debug("File written");
      } catch (NebuloException e) {
        logger_.warn("Could not write the file. ", e);
      }
      objects_.add(file);
      logger_.debug("Added object with objectId: " + file.getObjectId());

      synchronized (store_) {
        try {
          logger_.debug("Adding objects to store");
          store_.put(CODING_TEST_DATA_KEY, objects_);
          logger_.debug("Added objects to store");
        } catch (IOException e) {
          logger_.warn("Could not write objects list on hard disk", e);
        }
      }
    }

    logger_.debug("Sending objects to reader");
    networkQueue_.add(new TestNebuloObjectAddressesMessage(jobId_, myAddress_,
        CommAddress.getZero(),
        new LinkedList<NebuloAddress>(Lists.transform(objects_,
        new Function<NebuloFile, NebuloAddress>() {
            @Override
            public NebuloAddress apply(NebuloFile file) {
              return file.getAddress();
            }
          }))));
  }

  public synchronized void startReaderMode() {
    logger_.debug("Starting reader mode");
    mode_ = Mode.READER;
    timer_.scheduleRepeated(new CodingTestMessage(jobId_), CHECK_PERIOD_MILIS, CHECK_PERIOD_MILIS);
    logger_.debug("Scheduled reads");
  }

  public synchronized void stopReaderMode() {
    //FIXME a synchronizacja?
    logger_.debug("Stopping reader mode");
    mode_ = Mode.NONE;
    logger_.debug("Stopped reader mode");
  }

  public synchronized CodingTestResults getTestResults() {
    // FIXME synchronized?
    logger_.debug("Returning test results");
    CodingTestResults results =
        new CodingTestResults(successfulOperations_, unsuccessfulOperations_, receivedDataSizes_,
        recoveryDurations_, sentDataSizes_);
    logger_.debug("Results object: " + results);
    return results;
  }

  protected class CodingTestHelperModuleVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      // Try to read files from hard disk
      logger_.debug("Starting helper module");
      synchronized (store_) {
        logger_.debug("Got synchronized");
        List<NebuloFile> objects = store_.get(CODING_TEST_DATA_KEY);
        if (objects != null) {
          logger_.debug("Adding objects: " + objects);
          objects_.addAll(objects);
        }
      }

      CodingTestResults testResults = externalStore_.get(CODING_TEST_RESULTS_KEY + testId_ +
          myAddress_);
      if (testResults != null) {
        logger_.debug("Downloaded test results: " + testResults);
        successfulOperations_.putAll(testResults.successfulOperations_);
        unsuccessfulOperations_.putAll(testResults.unsuccessfulOperations_);
        receivedDataSizes_.addAll(testResults.receivedDataSizes_);
        recoveryDurations_.addAll(testResults.recoveryDurations_);
      }
    }

    public void visit(CodingTestMessage message) {
      if (mode_.equals(Mode.READER)) {
        logger_.debug("Starting read tests");

        Collections.shuffle(objectAddresses_);
        for (NebuloAddress address : objectAddresses_.subList(0,
            Math.min(NUMBER_OF_OBJECTS, objectAddresses_.size()))) {
          try {
            logger_.debug("Fetching next file: " + address);
            NebuloFile file = (NebuloFile) nebuloObjectFactory_.fetchExistingNebuloObject(address);
            file.read(0, file.getSize());
            logger_.debug("Module downloaded file successfully");
            if (!successfulOperations_.containsKey(address.getObjectId())) {
              successfulOperations_.put(address.getObjectId(), 0);
            }
            successfulOperations_.put(file.getObjectId(),
                successfulOperations_.get(file.getObjectId()) + 1);
          } catch (NebuloException e) {
            logger_.warn("File with address " + address + " could not be downloaded.", e);
            if (!unsuccessfulOperations_.containsKey(address.getObjectId())) {
              unsuccessfulOperations_.put(address.getObjectId(), 0);
            }
            unsuccessfulOperations_.put(address.getObjectId(),
                unsuccessfulOperations_.get(address.getObjectId()) + 1);
          }
        }
        updateInfoInExternalStore();
      }
    }

    public void visit(RecreateObjectFragmentsStatisticsMessage message) {
      logger_.debug("Received statistics about a recreation of fragments; sizes:" +
          message.getSizes() + "; time: " + message.getTime() + "; number of objects: " +
          message.getNumberOfObjects());
      for (Double size : message.getSizes().values()) {
        receivedDataSizes_.add(size);
        recoveryDurations_.add(message.getTime());
      }
      updateInfoInExternalStore();
    }

    public void visit(RecreationSentDataSizeMessage message) {
      //FIXME może jakoś to połączyć z tym kodem wyżej?
      logger_.debug("Received statistics about data sent during object recreation; size: " +
          message.getSize() + "; number of objects: " + message.getNumberOfObjects());
      double averageDataSize = ((double) message.getSize()) / message.getNumberOfObjects();
      for (int i = 0; i < message.getNumberOfObjects(); i++) {
        sentDataSizes_.add(averageDataSize);
      }
      updateInfoInExternalStore();
    }

    public void visit(TestNebuloObjectAddressesMessage message) {
      logger_.debug("Received message with test objects: " + message.getNebuloObjectAddresses() +
          " from peer " + message.getSourceAddress());
      if (message.getNebuloObjectAddresses() != null) {
        objectAddresses_.addAll(message.getNebuloObjectAddresses());
        updateObjectsInStore();
      }
    }
  }

  private synchronized void updateInfoInExternalStore() {
    CodingTestResults testResults = new CodingTestResults(successfulOperations_,
        unsuccessfulOperations_, receivedDataSizes_, recoveryDurations_, sentDataSizes_);
    logger_.debug("Putting test results to external store: " + testResults);
    try {
      externalStore_.put(CODING_TEST_RESULTS_KEY + testId_ + myAddress_, testResults);
    } catch (IOException e) {
      logger_.warn("Could not update test results in store", e);
    }
  }

  private synchronized void updateObjectsInStore() {
    synchronized (store_) {
      try {
        store_.put(CODING_TEST_DATA_KEY, objects_);
      } catch (IOException e) {
        logger_.warn("Could not save objects on the hard disk", e);
      }
    }
  }

  @Override
  protected synchronized void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private class CodingTestMessage extends Message {

    private static final long serialVersionUID = -947920523227252129L;

    public CodingTestMessage(String jobId) {
      super(jobId);
    }

  }

  @XmlRootElement
  public static class CodingTestResults implements Serializable {

    private static final long serialVersionUID = 2512942014449043119L;

    public final Map<ObjectId, Integer> successfulOperations_;
    public final Map<ObjectId, Integer> unsuccessfulOperations_;
    public final List<Double> receivedDataSizes_;
    public final List<Long> recoveryDurations_;
    public final List<Double> sentDataSizes_;

    public CodingTestResults(Map<ObjectId, Integer> successfulOperations,
        Map<ObjectId, Integer> unsuccessfulOperations, List<Double> receivedDataSizes,
        List<Long> recoveryDurations, List<Double> sentDataSizes) {
      successfulOperations_ = successfulOperations;
      unsuccessfulOperations_ = unsuccessfulOperations;
      receivedDataSizes_ = receivedDataSizes;
      recoveryDurations_ = recoveryDurations;
      sentDataSizes_ = sentDataSizes;
    }

    @Override
    public String toString() {
      return "CodingTestResults object with:" +
        "\nsuccessfulOperations_ = " + successfulOperations_ +
        "\nunsuccessfulOperations_ = " + unsuccessfulOperations_ +
        "\nreceivedDataSizes_ = " + receivedDataSizes_ +
        "\nrecoveryDurations_ = " + recoveryDurations_ +
        "\nsentDataSizes_ = " + sentDataSizes_;
    }
  }

}
