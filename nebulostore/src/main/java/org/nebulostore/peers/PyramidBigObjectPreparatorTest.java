package org.nebulostore.peers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.coding.pyramid.PyramidReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;

public final class PyramidBigObjectPreparatorTest {

  private static final int GROUPS_NUMBER = 5;
  private static final int OUT_SYMBOLS_NUMBER = 18;
  private static final int IN_SYMBOLS_NUMBER = 5;
  private static final int GLOBAL_REDUNDANCY_SIZE = 10;
  private static final int FINAL_SIZE = OUT_SYMBOLS_NUMBER + (GROUPS_NUMBER - 1) *
      (OUT_SYMBOLS_NUMBER - IN_SYMBOLS_NUMBER - GLOBAL_REDUNDANCY_SIZE);
  private static final int DATA_LENGTH = 10 * 1024 * 1024;

  private PyramidBigObjectPreparatorTest() { }

  public static void main(String[] args) throws NebuloException, InterruptedException {
    Thread.sleep(10000);
    ReplicaPlacementPreparator preparator = new PyramidReplicaPlacementPreparator(GROUPS_NUMBER,
        OUT_SYMBOLS_NUMBER, IN_SYMBOLS_NUMBER, GLOBAL_REDUNDANCY_SIZE);
    byte[] data = new byte[DATA_LENGTH];
    new Random().nextBytes(data);

    List<CommAddress> replicatorsAddresses = new ArrayList<>();
    for (int i = 0; i < FINAL_SIZE; i++) {
      replicatorsAddresses.add(CommAddress.newRandomCommAddress());
    }

    System.out.println("Starting!");
    System.out.println("Result: " + preparator.prepareObject(data, replicatorsAddresses));
  }

}
