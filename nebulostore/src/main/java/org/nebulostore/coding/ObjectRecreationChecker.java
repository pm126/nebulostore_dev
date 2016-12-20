package org.nebulostore.coding;

import java.util.List;
import java.util.Set;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Piotr Malicki
 */
public interface ObjectRecreationChecker {

  boolean isRecreationPossible(List<CommAddress> replicators,
      Set<CommAddress> availableReplicators);
}
