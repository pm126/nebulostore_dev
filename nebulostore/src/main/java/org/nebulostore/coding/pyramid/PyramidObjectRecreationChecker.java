package org.nebulostore.coding.pyramid;

import java.util.List;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.coding.ObjectRecreationChecker;
import org.nebulostore.communication.naming.CommAddress;

public class PyramidObjectRecreationChecker implements ObjectRecreationChecker {

  private static final int ADDITIONAL_GLOBAL_FRAGMENTS_NUMBER = 0;
  private static final int ADDITIONAL_LOCAL_FRAGMENTS_NUMBER = 0;

  private final int groupsNumber_;
  private final int outSymbolsNumber_;
  private final int inSymbolsNumber_;
  private final int globalRedundancySize_;

  @Inject
  public PyramidObjectRecreationChecker(
      @Named("coding.pyramid.groups-number") int groupsNumber,
      @Named("coding.pyramid.out-symbols-number") int outSymbolsNumber,
      @Named("coding.pyramid.in-symbols-number") int inSymbolsNumber,
      @Named("coding.pyramid.global-redundancy-size") int globalRedundancySize) {
    groupsNumber_ = groupsNumber;
    outSymbolsNumber_ = outSymbolsNumber;
    inSymbolsNumber_ = inSymbolsNumber;
    globalRedundancySize_ = globalRedundancySize;
  }

  @Override
  public boolean isRecreationPossible(List<CommAddress> replicators,
      Set<CommAddress> availableReplicators) {

    //Check number of fragments of original object and global redundancy
    int globalFragmentsNumber = 0;
    for (int i = 0; i < inSymbolsNumber_ + globalRedundancySize_; i++) {
      if (availableReplicators.contains(replicators.get(i))) {
        globalFragmentsNumber++;
      }
    }

    if (globalFragmentsNumber < inSymbolsNumber_ + ADDITIONAL_GLOBAL_FRAGMENTS_NUMBER) {
      return false;
    }

    //Check global fragments recoverable from local groups
    int groupSize = inSymbolsNumber_ / groupsNumber_;
    int localRedundancyStart = inSymbolsNumber_ + globalRedundancySize_;
    int localRedundancySize = outSymbolsNumber_ - localRedundancyStart;
    for (int i = 0; i < groupsNumber_; i++) {
      int availGroupFragmentsNumber = 0;
      for (int j = groupSize * i; j < groupSize * (i + 1); j++) {
        if (availableReplicators.contains(replicators.get(j))) {
          availGroupFragmentsNumber++;
        }
      }

      for (int j = localRedundancyStart + localRedundancySize * i;
          j < localRedundancyStart + localRedundancySize * (i + 1); j++) {
        if (availableReplicators.contains(replicators.get(j))) {
          availGroupFragmentsNumber++;
        }
      }

      if (availGroupFragmentsNumber < groupSize + ADDITIONAL_LOCAL_FRAGMENTS_NUMBER) {
        return false;
      }
    }

    return true;
  }

}
