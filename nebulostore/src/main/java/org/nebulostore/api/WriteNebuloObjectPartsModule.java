package org.nebulostore.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.PartialObjectWriter;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.Timer;

public class WriteNebuloObjectPartsModule extends WriteModule implements PartialObjectWriter {

  private Map<ObjectId, Map<CommAddress, EncryptedObject>> objectsMap_;
  private Map<ObjectId, List<String>> previousVersionSHAsMap_;
  private int confirmationsRequired_;

  @Inject
  public WriteNebuloObjectPartsModule(EncryptionAPI encryption, Timer timer) {
    super(encryption, timer);
  }

  @Override
  protected WriteModuleVisitor createVisitor() {
    return new WriteNebuloObjectPartsVisitor();
  }

  @Override
  public void writeObjects(Map<ObjectId, Map<CommAddress, EncryptedObject>> objectsMap,
      Map<ObjectId, List<String>> previousVersionSHAsMap, int confirmationsRequired,
      Map<ObjectId, Integer> objectSizesMap) {
    objectsMap_ = objectsMap;
    objectSizesMap_ = objectSizesMap;
    previousVersionSHAsMap_ = previousVersionSHAsMap;
    confirmationsRequired_ = confirmationsRequired;

    super.writeObject();
  }

  @Override
  protected boolean gotRequiredConfirmations(Set<CommAddress> confirmedReplicators) {
    return confirmedReplicators.size() >= confirmationsRequired_;
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  protected class WriteNebuloObjectPartsVisitor extends WriteModuleVisitor {

    public void visit(JobInitMessage message) {
      sendStoreQueries(objectsMap_, objectSizesMap_, previousVersionSHAsMap_, true,
          new HashMap<ObjectId, String>());
    }

  }


}
