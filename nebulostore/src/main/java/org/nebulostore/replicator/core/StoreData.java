package org.nebulostore.replicator.core;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public class StoreData implements Serializable {

  private static final long serialVersionUID = 3493488149204782292L;

  private final String remoteJobId_;
  private final Map<ObjectId, EncryptedObject> encryptedObjects_;
  private final Map<ObjectId, List<String>> previousVersionSHAsMap_;
  private final Map<ObjectId, String> newVersionSHAsMap_;
  private final Map<ObjectId, Integer> objectSizes_;

  public StoreData(String remoteJobId, Map<ObjectId, EncryptedObject> encryptedObjects,
      Map<ObjectId, List<String>> previousVersionSHAsMap,
      Map<ObjectId, String> newVersionSHAsMap, Map<ObjectId, Integer> objectSizes) {
    remoteJobId_ = remoteJobId;
    encryptedObjects_ = encryptedObjects;
    previousVersionSHAsMap_ = previousVersionSHAsMap;
    newVersionSHAsMap_ = newVersionSHAsMap;
    objectSizes_ = objectSizes;
  }

  public String getRemoteJobId() {
    return remoteJobId_;
  }

  public Map<ObjectId, EncryptedObject> getEncryptedObjects() {
    return encryptedObjects_;
  }

  public Map<ObjectId, List<String>> getPreviousVersionSHAsMap() {
    return previousVersionSHAsMap_;
  }

  public Map<ObjectId, String> getNewVersionSHAsMap() {
    return newVersionSHAsMap_;
  }

  public Map<ObjectId, Integer> getObjectSizes() {
    return objectSizes_;
  }

}
