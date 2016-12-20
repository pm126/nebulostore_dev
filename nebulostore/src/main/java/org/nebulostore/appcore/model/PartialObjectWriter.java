package org.nebulostore.appcore.model;

import java.util.List;
import java.util.Map;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.communication.naming.CommAddress;

public interface PartialObjectWriter extends GeneralObjectWriter {

  void writeObjects(Map<ObjectId, Map<CommAddress, EncryptedObject>> objectsMap,
      Map<ObjectId, List<String>> previousVersionSHAsMap, int confirmationsRequired,
      Map<ObjectId, Integer> objectSize);

}
