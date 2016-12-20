package org.nebulostore.appcore.addressing;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;

/**
 * A list of addresses of peers that share the same contract.
 */
public class ReplicationGroup implements Serializable, Comparable<ObjectId>, Iterable<CommAddress> {

  private static final long serialVersionUID = -2519006213860783596L;

  private static Logger logger_ = Logger.getLogger(ReplicationGroup.class);

  private final List<CommAddress> replicators_;

  // This group replicates objects with keys in [lowerBound_, upperBound_).
  private final BigInteger lowerBound_;
  private final BigInteger upperBound_;

  public ReplicationGroup(CommAddress[] replicators, BigInteger lBound,
      BigInteger uBound) {
    replicators_ = new ArrayList<CommAddress>(Arrays.asList(replicators));
    lowerBound_ = lBound;
    upperBound_ = uBound;
    logger_.info("Created new replication group with replicators: " + replicators_);
  }

  public int getSize() {
    return replicators_.size();
  }

  public List<CommAddress> getReplicators() {
    return new LinkedList<CommAddress>(replicators_);
  }

  @Override
  public Iterator<CommAddress> iterator() {
    return replicators_.iterator();
  }

  public void swapReplicators(CommAddress replicator, CommAddress newReplicator) {
    logger_.info("Replacing replicator " + replicator + " with new replicator: " + newReplicator);
    replicators_.set(replicators_.indexOf(replicator), newReplicator);
    logger_.info("Replicators currently in group: " + replicators_);
  }

  public boolean fitsIntoGroup(ObjectId objectId) {
    return lowerBound_.compareTo(objectId.getKey()) <= 0 &&
        upperBound_.compareTo(objectId.getKey()) > 0;
  }

  @Override
  public int compareTo(ObjectId id) {
    if (lowerBound_.compareTo(id.getKey()) == 1) {
      // Our interval is greater than objectId.
      return 1;
    } else if (upperBound_.compareTo(id.getKey()) <= 0) {
      // Our interval is less than objectId.
      return -1;
    } else {
      // This replication group is responsible for this objectId.
      return 0;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((lowerBound_ == null) ? 0 : lowerBound_.hashCode());
    /* TODO(pm) Logical comparisons of the objects should not depend on the current list of
    replicators */
    //result = prime * result + ((replicators_ == null) ? 0 : replicators_.hashCode());
    result = prime * result + ((upperBound_ == null) ? 0 : upperBound_.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ReplicationGroup other = (ReplicationGroup) obj;
    if (lowerBound_ == null) {
      if (other.lowerBound_ != null) {
        return false;
      }
    } else if (!lowerBound_.equals(other.lowerBound_)) {
      return false;
    }

    if (upperBound_ == null) {
      if (other.upperBound_ != null) {
        return false;
      }
    } else if (!upperBound_.equals(other.upperBound_)) {
      return false;
    }
    return true;
  }

  /**
   * Interval comparator. Return 0 for overlapping intervals.
   */
  static class IntervalComparator implements Comparator<ReplicationGroup>, Serializable {
    private static final long serialVersionUID = -5114789759938376551L;

    @Override
    public int compare(ReplicationGroup g1, ReplicationGroup g2) {
      if (g1.upperBound_.compareTo(g2.lowerBound_) <= 0) {
        return -1;
      } else if (g1.lowerBound_.compareTo(g2.upperBound_) >= 0) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  @Override
  public String toString() {
    return " ReplicationGroup [ lowerBound: " + lowerBound_ +
        ", upperBound: " + upperBound_ + ", replicators: " + replicators_ +
        " ] ";
  }

}
