package org.nebulostore.coding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import jersey.repackaged.com.google.common.collect.Sets;

import org.junit.Test;
import org.nebulostore.coding.pyramid.PyramidObjectRecreationChecker;
import org.nebulostore.communication.naming.CommAddress;

/**
 *
 * @author Piotr Malicki
 *
 */
public class PyramidObjectRecreationCheckerTest {

  private static final int GROUPS_NUMBER = 2;
  private static final int OUT_SYMBOLS_NUMBER = 5;
  private static final int IN_SYMBOLS_NUMBER = 2;
  private static final int GLOBAL_REDUNDANCY_SIZE = 2;
  private static final int FINAL_FRAGMENTS_NUMBER = IN_SYMBOLS_NUMBER + GLOBAL_REDUNDANCY_SIZE +
      (OUT_SYMBOLS_NUMBER - IN_SYMBOLS_NUMBER - GLOBAL_REDUNDANCY_SIZE) * GROUPS_NUMBER;

  private final ObjectRecreationChecker recreationChecker_ = new PyramidObjectRecreationChecker(
      GROUPS_NUMBER, OUT_SYMBOLS_NUMBER, IN_SYMBOLS_NUMBER, GLOBAL_REDUNDANCY_SIZE);

  @Test
  public void test() {
    List<CommAddress> replicators = new ArrayList<>();
    for (int i = 0; i < FINAL_FRAGMENTS_NUMBER; i++) {
      replicators.add(CommAddress.newRandomCommAddress());
    }

    for (int i = 0; i < FINAL_FRAGMENTS_NUMBER; i++) {
      for (int j = i + 1; j < FINAL_FRAGMENTS_NUMBER; j++) {
        System.out.println(i + ", " + j + ":");
        System.out.println(recreationChecker_.isRecreationPossible(
            replicators, Sets.newHashSet(replicators.get(i), replicators.get(j))));
      }
    }
  }

}
