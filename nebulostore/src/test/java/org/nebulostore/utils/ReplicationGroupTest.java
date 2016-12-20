package org.nebulostore.utils;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.communication.naming.CommAddress;

public class ReplicationGroupTest {

  private static final int COMM_ADDRESSES_NUMBER = 10;
  private static final BigInteger LOWER_BOUND = BigInteger.ZERO;
  private static final BigInteger UPPER_BOUND = BigInteger.valueOf(1000000L);

  @Test
  public void test() {
    CommAddress[] replicators = new CommAddress[COMM_ADDRESSES_NUMBER];
    for (int i = 0; i < COMM_ADDRESSES_NUMBER; i++) {
      replicators[i] = CommAddress.newRandomCommAddress();
    }
    ReplicationGroup group = new ReplicationGroup(replicators, LOWER_BOUND, UPPER_BOUND);
    Map<ReplicationGroup, Set<ObjectId>> map = new HashMap<>();
    map.put(group, new HashSet<ObjectId>());
    group.swapReplicators(replicators[0], CommAddress.newRandomCommAddress());
    System.out.println(map.containsKey(group));
    System.out.println(map.get(group));
  }

}
