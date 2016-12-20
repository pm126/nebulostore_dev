package org.nebulostore.coding.repetition;

import java.util.List;
import java.util.Set;

import org.nebulostore.coding.ObjectRecreationChecker;
import org.nebulostore.communication.naming.CommAddress;

/**
 *
 * @author Piotr Malicki
 *
 */
public class RepetitionObjectRecreationChecker implements ObjectRecreationChecker {

  @Override
  public boolean isRecreationPossible(List<CommAddress> replicators,
      Set<CommAddress> availableReplicators) {
    //FIXME dlaczego 2? Bo pewnie u siebie i jeszcze gdzieś
    return availableReplicators.size() >= 2;
  }

}
