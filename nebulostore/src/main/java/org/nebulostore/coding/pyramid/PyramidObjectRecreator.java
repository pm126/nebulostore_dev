package org.nebulostore.coding.pyramid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.backblaze.erasure.Galois;
import com.backblaze.erasure.ReedSolomon;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Piotr Malicki
 */
public class PyramidObjectRecreator implements ObjectRecreator {

  private static Logger logger_ = Logger.getLogger(PyramidObjectRecreator.class);

  private final List<CommAddress> replicators_ = new ArrayList<>();
  private final Map<CommAddress, Integer> replicatorIndices_ = new HashMap<>();
  private final Map<Integer, EncryptedObject> fragmentsMap_ = new HashMap<>();
  private final Set<Integer> neededFragments_ = new HashSet<>();
  private final List<FragmentRecoveryData> recoveryData_ = new LinkedList<>();
  // Set of numbers of fragments which are free in terms of communication. These are the already
  // downloaded fragments and the locally stored fragment.
  private final Set<Integer> freeFragments_ = new HashSet<>();

  private final int groupsNumber_;
  private final int outSymbolsNumber_;
  private final int inSymbolsNumber_;
  private final int globalRedundancySize_;
  private final ReedSolomon decoder_;
  private final CommAddress myAddress_;
  private int partSize_;

  private Integer fragmentNumber_ = -1;

  @Inject
  public PyramidObjectRecreator(@Named("coding.pyramid.groups-number") int groupsNumber,
      @Named("coding.pyramid.out-symbols-number") int outSymbolsNumber,
      @Named("coding.pyramid.in-symbols-number") int inSymbolsNumber,
      @Named("coding.pyramid.global-redundancy-size") int globalRedundancySize,
      CommAddress myAddress) {
    groupsNumber_ = groupsNumber;
    outSymbolsNumber_ = outSymbolsNumber;
    inSymbolsNumber_ = inSymbolsNumber;
    globalRedundancySize_ = globalRedundancySize;
    myAddress_ = myAddress;
    decoder_ = ReedSolomon.create(inSymbolsNumber_, outSymbolsNumber_ - inSymbolsNumber_);
  }

  private enum RecoveryType {
    GLOBAL_REED_SOLOMON, LOCAL_REED_SOLOMON, FRAGMENTS_SUM
  }

  private static class FragmentRecoveryData {
    public final Set<Integer> neededFragments_;
    public final Set<Integer> targetFragments_;
    public final RecoveryType recoveryType_;
    public final Integer groupNumber_;

    public FragmentRecoveryData(Set<Integer> neededFragments, Set<Integer> targetFragments,
        RecoveryType recoveryType, Integer groupNumber) {
      neededFragments_ = neededFragments;
      targetFragments_ = targetFragments;
      recoveryType_ = recoveryType;
      groupNumber_ = groupNumber;
    }

    @Override
    public String toString() {
      return recoveryType_ + " with neededFragments: " + neededFragments_ +
          " and targetFragments:" + targetFragments_;
    }
  }

  @Override
  public void initRecreator(Collection<CommAddress> replicators, Integer fragmentNumber) {
    replicators_.addAll(replicators);
    fragmentNumber_ = fragmentNumber;

    for (int i = 0; i < replicators_.size(); i++) {
      replicatorIndices_.put(replicators_.get(i), i);
    }

    if (replicatorIndices_.containsKey(myAddress_)) {
      // Add free (in terms of communication) local fragment to the free fragments' set
      freeFragments_.add(replicatorIndices_.get(myAddress_));
    }
  }

  @Override
  public boolean addNextFragment(EncryptedObject fragment, CommAddress replicator) {
    logger_.info("Adding fragment from replicator " + replicator);
    fragmentsMap_.put(replicatorIndices_.get(replicator), fragment);
    partSize_ = fragment.size();
    freeFragments_.add(replicatorIndices_.get(replicator));
    return fragmentsMap_.keySet().containsAll(neededFragments_);
  }

  @Override
  public EncryptedObject recreateObject(int objectSize) throws NebuloException {
    logger_.info("Trying to recreate the object");
    logger_.info("Current state:\n\tfragmentsMap_.keySet()=" +
        fragmentsMap_.keySet() + "\n\tneededFragments_=" + neededFragments_ + "\n\trecoveryData_=" +
        recoveryData_ + "\n\tpartsNumber_=" + partSize_ + "\n\tfragmentNumber_=" + fragmentNumber_);
    if (fragmentsMap_.keySet().containsAll(neededFragments_)) {
      if (fragmentNumber_ != null && fragmentsMap_.containsKey(fragmentNumber_)) {
        return fragmentsMap_.get(fragmentNumber_);
      }

      byte[][] fragments = new byte[outSymbolsNumber_][partSize_];
      boolean[] presentFragments = new boolean[outSymbolsNumber_];

      for (FragmentRecoveryData recoveryData : recoveryData_) {
        switch (recoveryData.recoveryType_) {
          case GLOBAL_REED_SOLOMON :
            int neededFragmentNumber = recoveryData.neededFragments_.iterator().next();
            System.arraycopy(fragmentsMap_.get(neededFragmentNumber).getEncryptedData(), 0,
                fragments[neededFragmentNumber], 0, partSize_);
            presentFragments[neededFragmentNumber] = true;
            break;
          case LOCAL_REED_SOLOMON :
            Set<Integer> lostFragments = new HashSet<>();
            int groupSize = inSymbolsNumber_ / groupsNumber_;
            int groupStart = recoveryData.groupNumber_ * groupSize;
            int localRedundancySize = outSymbolsNumber_ - inSymbolsNumber_ - globalRedundancySize_;
            int redundancyGroupStart = inSymbolsNumber_ + globalRedundancySize_ +
                recoveryData.groupNumber_ * localRedundancySize;
            for (int i = groupStart; i < redundancyGroupStart + localRedundancySize; i++) {
              if ((i < groupStart + groupSize || (i >= inSymbolsNumber_ &&
                  i < inSymbolsNumber_ + globalRedundancySize_) || i >= redundancyGroupStart) &&
                  !recoveryData.neededFragments_.contains(i)) {
                lostFragments.add(translateToSinglePartNumber(i));
              }
            }

            byte[][] toDecode = new byte[outSymbolsNumber_][partSize_];
            for (Integer partNumber : recoveryData.neededFragments_) {
              System.arraycopy(fragmentsMap_.get(partNumber).getEncryptedData(), 0,
                  toDecode[translateToSinglePartNumber(partNumber)], 0, partSize_);
            }

            decodeObject(toDecode, lostFragments);
            for (Integer fragmentNumber : recoveryData.targetFragments_) {
              if (fragmentNumber < inSymbolsNumber_ + globalRedundancySize_) {
                System.arraycopy(toDecode[fragmentNumber], 0, fragments[fragmentNumber], 0,
                    partSize_);
                presentFragments[fragmentNumber] = true;
              }
              fragmentsMap_.put(fragmentNumber, new EncryptedObject(Arrays.copyOf(
                  toDecode[translateToSinglePartNumber(fragmentNumber)], partSize_)));
            }
            break;
          case FRAGMENTS_SUM :
            byte[] sums = new byte[partSize_];
            for (Integer fragmentNumber : recoveryData.neededFragments_) {
              byte[] fragment = fragmentsMap_.get(fragmentNumber).getEncryptedData();
              for (int i = 0; i < partSize_; i++) {
                sums[i] = Galois.add(sums[i], fragment[i]);
              }
            }
            int fragmentNumber = recoveryData.targetFragments_.iterator().next();
            fragments[recoveryData.targetFragments_.iterator().next()] = sums;
            presentFragments[fragmentNumber] = true;
            break;
          default :
            break;
        }
      }

      if (fragmentNumber_ == null || (fragmentNumber_ < inSymbolsNumber_ +
          globalRedundancySize_ && !presentFragments[fragmentNumber_])) {

        // FIXME to niżej jest bez sensu, bo przekształcam tablicę na set, a w metodzie z powrotem

        // Prepare whole object
        Set<Integer> lostFragments = new HashSet<>();
        for (int i = 0; i < outSymbolsNumber_; i++) {
          if (!presentFragments[i]) {
            lostFragments.add(i);
          }
        }

        decodeObject(fragments, lostFragments);
        byte[] object = new byte[inSymbolsNumber_ * partSize_];
        for (int i = 0; i < inSymbolsNumber_; i++) {
          System.arraycopy(fragments[i], 0, object, i * partSize_, partSize_);
        }

        logger_.debug("Object successfully decoded");

        if (fragmentNumber_ == null) {
          return new EncryptedObject(Arrays.copyOfRange(object, 0, objectSize));
        } else {
          return new EncryptedObject(Arrays.copyOf(fragments[fragmentNumber_], partSize_));
        }
      }

      // Prepare only selected fragment
      return fragmentsMap_.get(fragmentNumber_);
    } else {
      throw new NebuloException("Not enough fragments to recreate the object.");
    }
  }

  @Override
  public List<CommAddress> calcReplicatorsToAsk(Set<CommAddress> askedReplicators) {
    logger_.info("Calculating replicators to ask, asked replicators set: " + askedReplicators);
    recoveryData_.clear();
    neededFragments_.clear();
    int globalFragmentsNumber = inSymbolsNumber_ + globalRedundancySize_;
    Set<CommAddress> result = new HashSet<>();

    logger_.info("Current free fragments set: " + freeFragments_);
    // Add fragments from asked replicators to the set of free fragments.
    for (CommAddress replicator : askedReplicators) {
      freeFragments_.add(replicatorIndices_.get(replicator));
    }
    logger_.info("Free fragments set: " + freeFragments_);

    if (fragmentNumber_ != null) {
      // fragment of original object or fragment from local redundancy group
      // At first try to download needed fragment directly
      if (replicators_.get(fragmentNumber_) != null) {
        result.add(replicators_.get(fragmentNumber_));
      } else if (fragmentNumber_ < inSymbolsNumber_ || fragmentNumber_ >= globalFragmentsNumber) {
        // We need only one specified fragment, so local group usage is preferred
        Set<Integer> neededFragments = Sets.newHashSet(fragmentNumber_);
        result.addAll(selectFragmentsReplicatorsFromLocalGroups(neededFragments, recoveryData_, 1,
            result));
        if (!neededFragments.isEmpty()) {
          result.clear();
        }
      }
    }

    // In case of failure and in all other cases recreating the full object is necessary
    if (result.isEmpty()) {
      result.addAll(selectReplicatorsFullObject(recoveryData_));
    }

    // Save the indices of needed fragments
    for (CommAddress replicator : result) {
      neededFragments_.add(replicatorIndices_.get(replicator));
    }

    // Remove fragments that we already have
    Iterator<CommAddress> iterator = result.iterator();
    while (iterator.hasNext()) {
      CommAddress address = iterator.next();
      if (fragmentsMap_.containsKey(replicatorIndices_.get(address))) {
        iterator.remove();
      }
    }

    //Try to remove redundant target fragments
    if (fragmentNumber_ == null) {
      int targetFragmentsNumber = 0;
      for (FragmentRecoveryData recoveryData : recoveryData_) {
        targetFragmentsNumber += recoveryData.targetFragments_.size();
      }

      if (targetFragmentsNumber > inSymbolsNumber_) {
        Set<Integer> fragmentsToRemove = new HashSet<>();
        Iterator<FragmentRecoveryData> recDataIterator = recoveryData_.iterator();
        while (recDataIterator.hasNext() && targetFragmentsNumber > inSymbolsNumber_) {
          FragmentRecoveryData recoveryData = recDataIterator.next();
          Integer fragmentNumber = recoveryData.neededFragments_.iterator().next();
          if (recoveryData.recoveryType_.equals(RecoveryType.GLOBAL_REED_SOLOMON) &&
              result.contains(replicators_.get(fragmentNumber))) {
            fragmentsToRemove.add(fragmentNumber);
            recDataIterator.remove();
            targetFragmentsNumber--;
          }
        }

        //Cancel removing fragments that are still needed
        for (FragmentRecoveryData recoveryData : recoveryData_) {
          Set<Integer> intersection = Sets.intersection(
              fragmentsToRemove, recoveryData.neededFragments_);
          fragmentsToRemove.removeAll(intersection);
        }

        for (Integer fragmentNumber : fragmentsToRemove) {
          result.remove(replicators_.get(fragmentNumber));
          neededFragments_.remove(fragmentNumber);
        }
      }
    }

    logger_.debug("\nCalculated replicators to ask: " + result);
    logger_.info("Current plan:\n\tfragmentsMap_.keySet()=" +
        fragmentsMap_.keySet() + "\n\tneededFragments_=" + neededFragments_ + "\n\trecoveryData_=" +
        recoveryData_ + "\n\tpartsNumber_=" + partSize_ + "\n\tfragmentNumber_=" + fragmentNumber_);

    return Lists.newLinkedList(result);
  }

  private Set<CommAddress> selectReplicatorsFullObject(List<FragmentRecoveryData> recoveryData) {
    Set<CommAddress> result = new HashSet<>();
    Set<Integer> unavailableFragments = new HashSet<>();

    // Full file is required, trying to select global fragments
    result.addAll(selectGlobalFragmentsReplicators(unavailableFragments, recoveryData));

    if (result.size() < inSymbolsNumber_) {
      // We need to add some local redundancy fragments
      // At first try to make the use of local groups
      result.addAll(selectFragmentsReplicatorsFromLocalGroups(unavailableFragments, recoveryData,
          inSymbolsNumber_ - result.size(), result));

      // Recover all remaining fragments by using local redundancy fragments' sum
      result.addAll(selectReplicatorsFromLocalSum(inSymbolsNumber_ -
          calcTargetReplicatorsNumber(recoveryData), recoveryData, result));

      if (calcTargetReplicatorsNumber(recoveryData) < inSymbolsNumber_) {
        result.clear();
        recoveryData.clear();
      }
    }

    return result;
  }

  private Set<CommAddress> selectGlobalFragmentsReplicators(Set<Integer> unavailableFragments,
      List<FragmentRecoveryData> recoveryData) {
    List<CommAddress> candidates = new ArrayList<>();
    int i = 0;
    while (i < inSymbolsNumber_ + globalRedundancySize_) {
      CommAddress replicator = replicators_.get(i);
      if (replicator != null) {
        candidates.add(replicator);

      } else if (i < inSymbolsNumber_) {
        unavailableFragments.add(i);
      }

      i++;
    }

    // Randomize order of indices with equal valuations
    Collections.shuffle(candidates);
    Collections.sort(candidates, new Comparator<CommAddress>() {
      @Override
      public int compare(CommAddress replicator1, CommAddress replicator2) {
        boolean isFree1 = freeFragments_.contains(replicatorIndices_.get(replicator1));
        boolean isFree2 = freeFragments_.contains(replicatorIndices_.get(replicator2));
        return isFree1 == isFree2 ? 0 : (isFree1 ? -1 : 1);
      }
    });

    Set<CommAddress> result = new HashSet<>();
    int j = 0;
    while (j < Math.min(inSymbolsNumber_, candidates.size())) {
      CommAddress replicator = candidates.get(j);
      Integer index = replicatorIndices_.get(replicator);
      recoveryData.add(new FragmentRecoveryData(Sets.newHashSet(index), Sets.newHashSet(index),
          RecoveryType.GLOBAL_REED_SOLOMON, null));
      result.add(replicator);
      j++;
    }

    return result;
  }

  private Set<CommAddress> selectFragmentsReplicatorsFromLocalGroups(
      Set<Integer> unavailableFragments, List<FragmentRecoveryData> recoveryData,
      int neededFragmentsNumber, Set<CommAddress> selectedReplicators) {
    logger_.debug("Unavailable fragments: " + unavailableFragments);
    logger_.debug("Recovery data: " + recoveryData);
    logger_.debug("Needed fragments number: " + neededFragmentsNumber);

    int globalFragmentsNumber = inSymbolsNumber_ + globalRedundancySize_;
    int localRedundancySize = outSymbolsNumber_ - globalFragmentsNumber;
    int groupSize = inSymbolsNumber_ / groupsNumber_;
    logger_.debug("Global fragments number: " + globalFragmentsNumber);
    logger_.debug("Local redundancy size: " + localRedundancySize);
    logger_.debug("Group size: " + groupSize);
    logger_.debug("Needed fragments: " + unavailableFragments);

    Set<CommAddress> presentReplicators = new HashSet<>();
    presentReplicators.addAll(selectedReplicators);
    for (Integer replicatorNumber : freeFragments_) {
      presentReplicators.add(replicators_.get(replicatorNumber));
    }

    Map<Integer, Set<Integer>> unavailFragmentsInGroups = new HashMap<>();
    // Assign each unavailable fragment to the proper local group
    for (Integer fragmentNumber : unavailableFragments) {
      int groupNumber;
      if (fragmentNumber < inSymbolsNumber_) {
        groupNumber = fragmentNumber / groupSize;
      } else {
        groupNumber = (fragmentNumber - globalFragmentsNumber) / localRedundancySize;
      }
      if (!unavailFragmentsInGroups.containsKey(groupNumber)) {
        unavailFragmentsInGroups.put(groupNumber, new HashSet<Integer>());
      }
      unavailFragmentsInGroups.get(groupNumber).add(fragmentNumber);
    }

    Map<Integer, Set<CommAddress>> neededReplicators = new HashMap<>();
    final Map<Integer, Set<CommAddress>> newReplicators = new HashMap<>();
    final Map<Integer, Set<Integer>> recoverableFragments = new HashMap<>();
    // Try to recover lost fragments using local redundancy
    for (Entry<Integer, Set<Integer>> groupEntry : unavailFragmentsInGroups.entrySet()) {
      logger_.debug("Next entry: <" + groupEntry.getKey() + ", " + groupEntry.getValue() + ">");
      int groupNumber = groupEntry.getKey();
      Set<Integer> unavailFragments = groupEntry.getValue();
      if (unavailFragments.size() <= localRedundancySize) {
        logger_.debug("Recovery possible");
        // Recovery is still possible
        Set<CommAddress> additionalReplicators = new HashSet<>();
        Set<CommAddress> presentGroupReplicators = new HashSet<>();
        int foundReplicators = 0;
        int groupStart = groupNumber * groupSize;
        int j = groupStart;
        logger_.debug("Group start: " + groupStart);
        while (j < groupStart + groupSize) {
          logger_.debug("Checking index: " + j);
          if (!unavailFragments.contains(j) && replicators_.get(j) != null) {
            if (!presentReplicators.contains(replicators_.get(j))) {
              additionalReplicators.add(replicators_.get(j));
            } else {
              presentGroupReplicators.add(replicators_.get(j));
            }
            logger_.debug("Found replicator!");
            foundReplicators++;
          }
          j++;
        }

        int groupRedundancyStart = globalFragmentsNumber + groupNumber * localRedundancySize;
        j = groupRedundancyStart;
        logger_.debug("Group redundancy start: " + j);
        while (j < groupRedundancyStart + localRedundancySize) {
          logger_.debug("Checking index: " + j);
          if (replicators_.get(j) != null) {
            if (!presentReplicators.contains(replicators_.get(j))) {
              additionalReplicators.add(replicators_.get(j));
            } else {
              presentGroupReplicators.add(replicators_.get(j));
            }
            logger_.debug("Found replicator!");
            foundReplicators++;
          }
          j++;
        }

        logger_.debug("Additional replicators: " + additionalReplicators);
        if (foundReplicators >= groupSize) {
          // All fragments can be recovered
          newReplicators.put(
              groupNumber,
              new HashSet<CommAddress>(ImmutableSet.copyOf(Iterables.limit(additionalReplicators,
              Math.max(groupSize - (foundReplicators - additionalReplicators.size()), 0)))));
          neededReplicators.put(groupNumber,
              new HashSet<CommAddress>(newReplicators.get(groupNumber)));
          neededReplicators.get(groupNumber).addAll(
              ImmutableSet.copyOf(Iterables.limit(presentGroupReplicators, groupSize)));
          recoverableFragments.put(groupNumber, groupEntry.getValue());
        }
      }
    }

    List<Integer> groupNumbers = new ArrayList<>(newReplicators.keySet());

    int recoverableFragmentsNumber = 0;
    for (Set<Integer> fragments : recoverableFragments.values()) {
      recoverableFragmentsNumber += fragments.size();
    }

    int redundantFragmentsNumber = recoverableFragmentsNumber - neededFragmentsNumber;
    Set<Integer> redundantGroups = solveKnapsack(groupNumbers, newReplicators, recoverableFragments,
        redundantFragmentsNumber);

    Set<CommAddress> result = new HashSet<>();
    for (Integer groupNumber : groupNumbers) {
      if (!redundantGroups.contains(groupNumber)) {
        result.addAll(neededReplicators.get(groupNumber));
        Set<Integer> additionalFragments = new HashSet<>();
        for (CommAddress address : neededReplicators.get(groupNumber)) {
          additionalFragments.add(replicatorIndices_.get(address));
        }
        recoveryData.add(new FragmentRecoveryData(additionalFragments, new HashSet<Integer>(
            recoverableFragments.get(groupNumber)), RecoveryType.LOCAL_REED_SOLOMON, groupNumber));
        unavailableFragments.removeAll(recoverableFragments.get(groupNumber));
      }
    }

    return result;
  }

  private Set<CommAddress> selectReplicatorsFromLocalSum(int neededFragmentsNumber,
      List<FragmentRecoveryData> recoveryData, Set<CommAddress> selectedReplicators) {
    Set<CommAddress> presentReplicators = new HashSet<>(selectedReplicators);
    for (Integer replicatorNumber : freeFragments_) {
      presentReplicators.add(replicators_.get(replicatorNumber));
    }

    Set<CommAddress> result = new HashSet<>();

    int globalFragmentsNumber = inSymbolsNumber_ + globalRedundancySize_;
    int localRedundancySize = outSymbolsNumber_ - globalFragmentsNumber;

    Map<Integer, Set<Integer>> neededFragmentsMap = new HashMap<>();
    final Map<Integer, Set<CommAddress>> newReplicatorsMap = new HashMap<>();
    int i = 0;
    while (i < localRedundancySize) {
      Set<CommAddress> newReplicators = new HashSet<>();
      Set<Integer> localFragments = new HashSet<>();
      for (int j = 0; j < groupsNumber_; j++) {
        int index = globalFragmentsNumber + j * localRedundancySize + i;
        if (replicators_.get(index) != null) {
          if (!presentReplicators.contains(replicators_.get(index))) {
            newReplicators.add(replicators_.get(index));
          }
          localFragments.add(index);
        }
      }

      if (localFragments.size() == groupsNumber_) {
        neededFragmentsMap.put(i, localFragments);
        newReplicatorsMap.put(i, newReplicators);
      }
      i++;
    }

    List<Integer> redundancyIndices = new ArrayList<>(neededFragmentsMap.keySet());
    // Randomize order of indices with equal valuations
    Collections.shuffle(redundancyIndices);
    Collections.sort(redundancyIndices, new Comparator<Integer>() {
      @Override
      public int compare(Integer index1, Integer index2) {
        return Integer.compare(newReplicatorsMap.get(index1).size(), newReplicatorsMap.get(index2)
            .size());
      }
    });

    int newFragmentsNumber = 0;
    Iterator<Integer> iterator = redundancyIndices.iterator();
    while (newFragmentsNumber < neededFragmentsNumber && iterator.hasNext()) {
      int redundancyIndex = iterator.next();
      Set<CommAddress> neededReplicators = new HashSet<>();
      for (Integer fragmentNumber : neededFragmentsMap.get(redundancyIndex)) {
        neededReplicators.add(replicators_.get(fragmentNumber));
      }
      result.addAll(neededReplicators);
      recoveryData.add(new FragmentRecoveryData(neededFragmentsMap.get(redundancyIndex), Sets
          .newHashSet(globalFragmentsNumber + redundancyIndex), RecoveryType.FRAGMENTS_SUM, null));
    }

    return result;
  }

  private void decodeObject(byte[][] toDecode, Set<Integer> lostPositions) {
    boolean[] fragmentsPresent = new boolean[outSymbolsNumber_];
    Arrays.fill(fragmentsPresent, true);
    for (int position : lostPositions) {
      fragmentsPresent[position] = false;
    }
    decoder_.decodeMissing(toDecode, fragmentsPresent, 0, partSize_);
  }

  private int translateToSinglePartNumber(Integer fragmentNumber) {
    if (fragmentNumber < outSymbolsNumber_) {
      return fragmentNumber;
    }

    int localRedundancySize = outSymbolsNumber_ - inSymbolsNumber_ - globalRedundancySize_;
    return inSymbolsNumber_ + globalRedundancySize_ + (fragmentNumber - outSymbolsNumber_) %
        localRedundancySize;
  }

  @Override
  public void removeReplicator(CommAddress replicator) {
    logger_.info("Removing replicator: " + replicator);
    Integer index = replicatorIndices_.remove(replicator);
    if (index != null) {
      replicators_.set(index, null);
      freeFragments_.remove(index);
    } else {
      logger_.warn("Could not remove replicator: no such address.");
    }
  }

  @Override
  public void clearReceivedFragments() {
    // Remove all replicators that had sent their fragments
    for (Integer replicatorNumber : fragmentsMap_.keySet()) {
      if (replicators_.get(replicatorNumber) != null) {
        replicatorIndices_.remove(replicators_.get(replicatorNumber));
      }
      replicators_.set(replicatorNumber, null);
      freeFragments_.remove(replicatorNumber);
    }
    fragmentsMap_.clear();
  }

  private int calcTargetReplicatorsNumber(List<FragmentRecoveryData> recoveryData) {
    int targetReplicatorsNum = 0;
    for (FragmentRecoveryData recData : recoveryData) {
      targetReplicatorsNum += recData.targetFragments_.size();
    }
    return targetReplicatorsNum;
  }

  private Set<Integer> solveKnapsack(List<Integer> groupNumbers, Map<Integer,
      Set<CommAddress>> newReplicators, Map<Integer, Set<Integer>> recoverableFragments,
      int redundantFragmentsNumber) {
    int[][] subKnapsackValues = new int[groupNumbers.size()][redundantFragmentsNumber + 1];
    for (int i = 0; i < groupNumbers.size(); i++) {
      for (int j = 0; j <= redundantFragmentsNumber; j++) {
        if (i == 0) {
          subKnapsackValues[i][j] = 0;
        } else {
          int value = newReplicators.get(groupNumbers.get(i)).size();
          int weight = recoverableFragments.get(groupNumbers.get(i)).size();
          if (weight > j) {
            subKnapsackValues[i][j] = subKnapsackValues[i - 1][j];
          } else {
            subKnapsackValues[i][j] = Math.max(subKnapsackValues[i - 1][j],
                value + subKnapsackValues[i - 1][j - weight]);
          }
        }
      }
    }

    int nextWeightSum = redundantFragmentsNumber;
    Set<Integer> selectedGroups = new HashSet<>();
    for (int i = groupNumbers.size() - 1; i > 0; i--) {
      int value = newReplicators.get(groupNumbers.get(i)).size();
      int weight = recoverableFragments.get(groupNumbers.get(i)).size();
      if (weight <= nextWeightSum && subKnapsackValues[i][nextWeightSum] - value ==
          subKnapsackValues[i - 1][nextWeightSum - weight]) {
        //Group is present in the best set
        selectedGroups.add(groupNumbers.get(i));
        nextWeightSum -= weight;
      }
    }
    return selectedGroups;
  }

}
