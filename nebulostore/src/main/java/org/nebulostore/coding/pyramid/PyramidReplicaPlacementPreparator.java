package org.nebulostore.coding.pyramid;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.backblaze.erasure.ReedSolomon;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ReplicaPlacementData;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Replica placement preparator using (n, k) pyramid code.
 *
 * @author Piotr Malicki
 */
public class PyramidReplicaPlacementPreparator implements ReplicaPlacementPreparator {

  private static Logger logger_ = Logger.getLogger(PyramidReplicaPlacementPreparator.class);

  private final int groupsNumber_;
  private final int outSymbolsNumber_;
  private final int inSymbolsNumber_;
  private final int globalRedundancySize_;
  private final ReedSolomon encoder_;

  @Inject
  public PyramidReplicaPlacementPreparator(
      @Named("coding.pyramid.groups-number") int groupNumber,
      @Named("coding.pyramid.out-symbols-number") int outSymbolsNumber,
      @Named("coding.pyramid.in-symbols-number") int inSymbolsNumber,
      @Named("coding.pyramid.global-redundancy-size") int globalRedundancySize) {
    groupsNumber_ = groupNumber;
    outSymbolsNumber_ = outSymbolsNumber;
    inSymbolsNumber_ = inSymbolsNumber;
    globalRedundancySize_ = globalRedundancySize;
    encoder_ = ReedSolomon.create(inSymbolsNumber_, outSymbolsNumber_ - inSymbolsNumber_);
  }

  @Override
  public ReplicaPlacementData prepareObject(byte[] encryptedObject,
      List<CommAddress> replicatorsAddresses) throws NebuloException {
    logger_.debug("start preparing object");
    //logger_.debug("Current object: " + Arrays.toString(encryptedObject));

    int redundancySize = outSymbolsNumber_ - inSymbolsNumber_;
    int localRedundancySize = redundancySize - globalRedundancySize_;
    int finalSize = inSymbolsNumber_ + globalRedundancySize_ + groupsNumber_ * localRedundancySize;

    if (replicatorsAddresses.size() == finalSize) {

      int partSize = (int) Math.ceil(encryptedObject.length / (double) inSymbolsNumber_);

      byte[][] toEncode = new byte[outSymbolsNumber_][partSize];
      for (int i = 0; i < inSymbolsNumber_ && i * partSize < encryptedObject.length; i++) {
        System.arraycopy(encryptedObject, i * partSize, toEncode[i], 0,
            Math.min(partSize, encryptedObject.length - i * partSize));
      }

      Map<CommAddress, EncryptedObject> placementMap = new HashMap<>();
      for (CommAddress replicator : replicatorsAddresses) {
        placementMap.put(replicator, new EncryptedObject(new byte[partSize]));
      }
      logger_.debug("placement map initialized");

      encoder_.encodeParity(toEncode, 0, partSize);
      //logger_.debug("Encoded object: " + Arrays.toString(toEncode));
      for (int i = 0; i < inSymbolsNumber_ + globalRedundancySize_; i++) {
        System.arraycopy(toEncode[i], 0,
            placementMap.get(replicatorsAddresses.get(i)).getEncryptedData(),
            0, partSize);
      }

      int groupSize = inSymbolsNumber_ / groupsNumber_;
      byte[][] groupObject = new byte[outSymbolsNumber_][partSize];
      for (int i = 0; i < groupsNumber_; i++) {
        for (int j = 0; j < outSymbolsNumber_; j++) {
          if (j < i * groupSize || j >= (i + 1) * groupSize) {
            Arrays.fill(groupObject[j], 0, partSize, (byte) 0);
          } else {
            System.arraycopy(toEncode[j], 0, groupObject[j], 0, partSize);
          }
        }

        encoder_.encodeParity(groupObject, 0, partSize);
        for (int j = inSymbolsNumber_ + globalRedundancySize_; j < outSymbolsNumber_; j++) {
          System.arraycopy(groupObject[j], 0,
              placementMap.get(replicatorsAddresses.get(j + i * localRedundancySize)).
              getEncryptedData(), 0, partSize);
        }
      }

      /*logger_.debug("Placement map:");
      for (Entry<CommAddress, EncryptedObject> entry : placementMap.entrySet()) {
        logger_.debug("\t<" + entry.getKey() + ", " +
            Arrays.toString(entry.getValue().getEncryptedData()) + ">");
      }*/

      return new ReplicaPlacementData(placementMap);
    } else {
      throw new NebuloException("Wrong number of replicators. Expected: " + outSymbolsNumber_ +
          ", got: " + replicatorsAddresses.size());
    }
  }
}
