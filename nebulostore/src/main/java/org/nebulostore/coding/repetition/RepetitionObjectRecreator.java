package org.nebulostore.coding.repetition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Simple object recreator used for repetition code (full object replication).
 *
 * @author Piotr Malicki
 *
 */
public class RepetitionObjectRecreator implements ObjectRecreator {

  private EncryptedObject object_;
  private final List<CommAddress> replicators_ = new ArrayList<>();
  private final CommAddress myAddress_;

  @Inject
  public RepetitionObjectRecreator(CommAddress myAddress) {
    myAddress_ = myAddress;
  }

  @Override
  public boolean addNextFragment(EncryptedObject fragment, CommAddress replicator) {
    if (replicators_.remove(replicator)) {
      object_ = fragment;
    }
    return object_ != null;
  }

  @Override
  public EncryptedObject recreateObject(int objectSize) throws NebuloException {
    if (object_ == null) {
      throw new NebuloException("Not enough fragments to recreate the object.");
    }
    return object_;
  }

  @Override
  public void initRecreator(Collection<CommAddress> replicators, Integer fragmentNumber) {
    replicators_.addAll(replicators);
  }

  @Override
  public List<CommAddress> calcReplicatorsToAsk(Set<CommAddress> askedReplicators) {
    if (replicators_.isEmpty()) {
      return new ArrayList<CommAddress>();
    }
    if (replicators_.contains(myAddress_)) {
      return Lists.newArrayList(myAddress_);
    }
    return Lists.newArrayList(replicators_.get(0));
  }

  @Override
  public void removeReplicator(CommAddress replicator) {
    replicators_.remove(replicator);
  }

  @Override
  public void clearReceivedFragments() {
    object_ = null;
  }

}
