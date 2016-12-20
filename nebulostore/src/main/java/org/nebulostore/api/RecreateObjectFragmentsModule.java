package org.nebulostore.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.RecreateObjectFragmentsModule.FragmentsData;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.replicator.messages.SendObjectsMessage;

/**
 * @author Piotr Malicki
 */
public class RecreateObjectFragmentsModule extends GetModule<FragmentsData> {

  private static Logger logger_ = Logger.getLogger(RecreateObjectFragmentsModule.class);

  private final MessageVisitor visitor_;

  private boolean includeMyAddress_;
  private final Map<ObjectId, Integer> objectsSizes_ = new HashMap<>();
  protected CommAddress peerAddress_;

  @Inject
  public RecreateObjectFragmentsModule(
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    setDispatcherQueue(dispatcherQueue);
    visitor_ = createVisitor();
  }

  public void getAndCalcFragments(Set<NebuloAddress> nebuloAddresses, CommAddress address,
      boolean includeMyAddress, GetModuleMode mode) {
    peerAddress_ = address;
    includeMyAddress_ = includeMyAddress;
    fetchObject(nebuloAddresses, mode);
  }

  protected MessageVisitor createVisitor() {
    return new RecreateObjectFragmentVisitor();
  }


  protected class RecreateObjectFragmentVisitor extends GetModuleVisitor {

    @Override
    public void visit(SendObjectsMessage message) {
      objectsSizes_.putAll(message.getObjectsSizes());
      try {
        if (tryRecreateEncryptedObjects(message)) {
          endWithResult();
        }
      } catch (NebuloException e) {
        timer_.cancelTimer();
        endWithError(e);
        return;
      }
    }

    @Override
    protected void initRecreators() throws NebuloException {
      int fragmentNumber = replicationGroup_.getReplicators().indexOf(peerAddress_);
      if (fragmentNumber == -1) {
        throw new NebuloException("Error while initializing recreator: given peer is " +
            " not present in the replication group.");
      }
      List<CommAddress> replicators = replicationGroup_.getReplicators();
      for (ObjectId objectId : objectIds_) {
        ObjectRecreator recreator = recreatorProvider_.get();
        recreator.initRecreator(replicators, fragmentNumber);
        if (!includeMyAddress_) {
          recreator.removeReplicator(myAddress_);
        }
        recreators_.put(objectId, recreator);
      }
    }

    protected void endModuleWithSuccess(FragmentsData fragmentsData) {
      endWithSuccess(fragmentsData);
    }

    @Override
    protected void endWithResult() {
      timer_.cancelTimer();
      endModuleWithSuccess(new FragmentsData(downloadedObjects_, currentVersionsMap_,
          Maps.filterKeys(objectsSizes_, new Predicate<ObjectId>() {
            @Override
            public boolean apply(ObjectId objectId) {
              return downloadedObjects_.containsKey(objectId);
            }
          }))
      );
    }
  }

  public static class FragmentsData {
    public Map<ObjectId, EncryptedObject> fragmentsMap_;
    public Map<ObjectId, List<String>> versionsMap_;
    public Map<ObjectId, Integer> objectsSizes_;

    public FragmentsData(Map<ObjectId, EncryptedObject> fragmentsMap,
        Map<ObjectId, List<String>> versionsMap, Map<ObjectId, Integer> objectsSizes) {
      fragmentsMap_ = fragmentsMap;
      versionsMap_ = versionsMap;
      objectsSizes_ = objectsSizes;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
