package org.nebulostore.coding.repetition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ReplicaPlacementData;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;

public class RepetitionReplicaPlacementPreparator implements ReplicaPlacementPreparator {

  @Override
  public ReplicaPlacementData prepareObject(byte[] encryptedObject,
      List<CommAddress> replicatorsAddresses) {
    Map<CommAddress, EncryptedObject> placementMap = new HashMap<>();
    for (CommAddress replicator : replicatorsAddresses) {
      placementMap.put(replicator, new EncryptedObject(encryptedObject));
    }
    return new ReplicaPlacementData(placementMap);
  }

}
