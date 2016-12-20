package org.nebulostore.appcore.addressing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * List of replication groups.
 */
public class ContractList implements Serializable {
  private static final long serialVersionUID = 8829107129515732846L;
  private final List<ReplicationGroup> groups_;

  public ContractList() {
    groups_ = new ArrayList<ReplicationGroup>();
  }

  // Insert group in the correct position.
  public void addGroup(ReplicationGroup group) throws IntervalCollisionException {
    // Returns (-(insertion point) - 1) when not found.
    int where = Collections.binarySearch(groups_, group, new ReplicationGroup.IntervalComparator());
    if (where >= 0) {
      throw new IntervalCollisionException();
    } else {
      groups_.add(-where - 1, group);
    }
  }

  // Return whether removal was successful.
  public boolean removeGroup(int index) {
    if (index < groups_.size() && index >= 0) {
      groups_.remove(index);
      return true;
    } else {
      return false;
    }
  }

  // Find and return replication group responsible for the given ID.
  public ReplicationGroup getGroup(ObjectId id) {
    int where = Collections.binarySearch(groups_, id);
    if (where < 0) {
      return null;
    } else {
      return groups_.get(where);
    }
  }

  @Override
  public String toString() {
    return " ContractList [ " + groups_ + " ] ";
  }
}
