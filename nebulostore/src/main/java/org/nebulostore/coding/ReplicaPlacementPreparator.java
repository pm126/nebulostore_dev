package org.nebulostore.coding;

import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Interface for replica placement preparators.
 *
 * @author Piotr Malicki
 *
 */
public interface ReplicaPlacementPreparator {

  /**
   * Encode given object and prepare placement data informing at which replicator each part should
   * be placed.
   *
   * @param encryptedObject object to place at replicators
   * @param replicatorsAddresses addresses of all object's replicators
   * @return
   */
  ReplicaPlacementData prepareObject(byte[] encryptedObject,
      List<CommAddress> replicatorsAddresses) throws NebuloException;
}
