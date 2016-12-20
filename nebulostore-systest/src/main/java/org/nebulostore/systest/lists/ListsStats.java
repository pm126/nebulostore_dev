package org.nebulostore.systest.lists;

import java.util.ArrayList;
import java.util.List;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.conductor.CaseStatistics;

/**
 * Statistics for lists test.
 *
 * @author Bolek Kulbabinski
 */
public final class ListsStats extends CaseStatistics {
  private static final long serialVersionUID = -2676033693308257318L;
  private int nTriedFiles_;
  private List<NebuloAddress> unavailableFiles_ = new ArrayList<NebuloAddress>();

  public void incTriedFiles() {
    ++nTriedFiles_;
  }

  public int getNTriedFiles() {
    return nTriedFiles_;
  }

  public void addAddress(NebuloAddress address) {
    unavailableFiles_.add(address);
  }

  public List<NebuloAddress> getAddresses() {
    return unavailableFiles_;
  }
}
