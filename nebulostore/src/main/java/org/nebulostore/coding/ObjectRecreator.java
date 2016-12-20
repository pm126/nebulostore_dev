package org.nebulostore.coding;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Interface for object recreators that are used to recreate objects based on theirs fragments
 * downloaded from other peers. Recreation is done using proper coding algorithm.
 *
 * @author Piotr Malicki
 *
 */
public interface ObjectRecreator {

  /**
   * Init recreator by setting replicators to ask for object fragments and required object
   * fragment's index (null if whole objects are required). The order of replicators in given
   * collection is important. This method has to be called before using the recreator.
   *
   * @param replicators replicators list
   * @param fragmentNumber index of fragments to recreate (null if whole object is needed)
   */
  void initRecreator(Collection<CommAddress> replicators, Integer fragmentNumber);

  /**
   * Add next file fragment to the recreator. This method returns true if the recreator has already
   * received enough fragments to recreate the object, false otherwise.
   *
   * @param fragment
   *          next fragment of the object
   * @param replicator
   *          address of peer which sent this fragment
   * @return
   */
  boolean addNextFragment(EncryptedObject fragment, CommAddress replicator);

  /**
   * Recreate the object based on received fragments and return it. This method should be called
   * only after addNextFragment method returned true at least once. In the other case, an exception
   * is thrown.
   *
   * @return recreated object
   */
  EncryptedObject recreateObject(int objectSize) throws NebuloException;

  /**
   * Calculate replicators to ask for object fragments. The returned replicators list should be as
   * short as it's possible.
   *
   * @param askedReplicators
   *          replicators that have been already asked for fragments. The recreator treats fragments
   *          from these replicators in the same manner as already downloaded fragments
   *
   * @return list of replicators to ask
   */
  List<CommAddress> calcReplicatorsToAsk(Set<CommAddress> askedReplicators);

  /**
   * Remove replicator from possible peers to ask for fragment. This method should be used when the
   * caller didn't manage to receive object fragment from given peer.
   *
   * @param replicator
   */
  void removeReplicator(CommAddress replicator);

  /**
   * Ignore all fragments that had been received until now.
   */
  void clearReceivedFragments();

}
