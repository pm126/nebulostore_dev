package org.nebulostore.coding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.pyramid.PyramidObjectRecreator;
import org.nebulostore.coding.pyramid.PyramidReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PyramidObjectRecreatorTest {

  public static final int GROUPS_NUMBER = 4;
  public static final int OUT_SYMBOLS_NUMBER = 30;
  public static final int IN_SYMBOLS_NUMBER = 16;
  public static final int GLOBAL_REDUNDANCY_SIZE = 4;
  public static final int LOCAL_REDUNDANCY_SIZE = OUT_SYMBOLS_NUMBER - IN_SYMBOLS_NUMBER -
      GLOBAL_REDUNDANCY_SIZE;
  public static final int FINAL_SIZE = IN_SYMBOLS_NUMBER + GLOBAL_REDUNDANCY_SIZE + GROUPS_NUMBER *
      LOCAL_REDUNDANCY_SIZE;
  public static final int ORIGINAL_FRAGMENT_NUMBER = 5;
  public static final int GLOBAL_REDUNDANCY_FRAGMENT = 18;
  public static final int LOCAL_REDUNDANCY_FRAGMENT = 30;

  public static String message_ = CryptoUtils.getRandomString();
  public static List<CommAddress> replicators_ = new LinkedList<>();
  public static ReplicaPlacementData placementData_;

  @BeforeClass
  public static void initializeTestVariables() throws NebuloException {
    for (int i = 0; i < FINAL_SIZE; i++) {
      replicators_.add(new CommAddress(Integer.toString(i)));
    }

    PyramidReplicaPlacementPreparator placementPreparator =
        new PyramidReplicaPlacementPreparator(GROUPS_NUMBER, OUT_SYMBOLS_NUMBER, IN_SYMBOLS_NUMBER,
        GLOBAL_REDUNDANCY_SIZE);
    placementData_ = placementPreparator.prepareObject(message_.getBytes(), replicators_);

  }

  @Test
  public void shouldRecoverOriginalFragmentProperly() throws NebuloException {
    shouldRecoverEncryptedObjectProperly(ORIGINAL_FRAGMENT_NUMBER);
  }

  @Test
  public void shouldRecoverGlobalRedundancyFragmentProperly() throws NebuloException {
    shouldRecoverEncryptedObjectProperly(GLOBAL_REDUNDANCY_FRAGMENT);
  }

  @Test
  public void shouldRecoverLocalRedundancyFragmentproperly() throws NebuloException {
    shouldRecoverEncryptedObjectProperly(LOCAL_REDUNDANCY_FRAGMENT);
  }

  @Test
  public void shouldRecoverWholeObjectProperly() throws NebuloException {
    shouldRecoverEncryptedObjectProperly(null);
  }

  @Test
  public void shouldRecoverObjectWithRandomFailsProperly() throws NebuloException {
    List<CommAddress> replicatorsCopy = new ArrayList<CommAddress>(replicators_);
    Collections.shuffle(replicatorsCopy);
    List<CommAddress> unavailReplicators =
        replicatorsCopy.subList(0, OUT_SYMBOLS_NUMBER - IN_SYMBOLS_NUMBER);

    PyramidObjectRecreator recreator =
        new PyramidObjectRecreator(GROUPS_NUMBER, OUT_SYMBOLS_NUMBER, IN_SYMBOLS_NUMBER,
        GLOBAL_REDUNDANCY_SIZE, replicators_.get(new Random().nextInt(replicators_.size())));
    recreator.initRecreator(replicators_, null);

    List<CommAddress> replicatorsToAsk = recreator.calcReplicatorsToAsk(new HashSet<CommAddress>());
    EncryptedObject result = null;
    while (!replicatorsToAsk.isEmpty() && result == null) {
      Set<CommAddress> askedReplicators = new HashSet<>(replicatorsToAsk);
      for (CommAddress replicator : replicatorsToAsk) {
        askedReplicators.remove(replicator);
        if (unavailReplicators.contains(replicator)) {
          recreator.removeReplicator(replicator);
          replicatorsToAsk = recreator.calcReplicatorsToAsk(askedReplicators);
          break;
        } else if (recreator.addNextFragment(placementData_.getReplicaPlacementMap()
            .get(replicator), replicator)) {
          result = recreator.recreateObject(message_.getBytes().length);
          break;
        }
      }
    }

    assertNotNull(result);
    assertArrayEquals(message_.getBytes(), result.getEncryptedData());
  }

  private void shouldRecoverEncryptedObjectProperly(Integer fragmentNumber) throws NebuloException {
    PyramidObjectRecreator recreator =
        new PyramidObjectRecreator(GROUPS_NUMBER, OUT_SYMBOLS_NUMBER, IN_SYMBOLS_NUMBER,
        GLOBAL_REDUNDANCY_SIZE, replicators_.get(new Random().nextInt(replicators_.size())));
    recreator.initRecreator(replicators_, fragmentNumber);
    if (fragmentNumber != null) {
      recreator.removeReplicator(replicators_.get(fragmentNumber));
    }

    List<CommAddress> replicatorsToAsk = recreator.calcReplicatorsToAsk(new HashSet<CommAddress>());
    boolean canRecover = false;
    for (CommAddress address : replicatorsToAsk) {
      canRecover =
          recreator.addNextFragment(placementData_.getReplicaPlacementMap().get(address), address);
    }

    assertTrue(canRecover);

    EncryptedObject encryptedObject = recreator.recreateObject(message_.getBytes().length);
    if (fragmentNumber == null) {
      assertArrayEquals(message_.getBytes(), encryptedObject.getEncryptedData());
    } else {
      assertArrayEquals(placementData_.getReplicaPlacementMap().get(
          replicators_.get(fragmentNumber)).getEncryptedData(), encryptedObject.getEncryptedData());
    }
  }

}
