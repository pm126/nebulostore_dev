package org.nebulostore.dfuntest.coding.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.RecreateObjectFragmentsModule;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.coding.communication.messages.SendObjectMessageWithSize;
import org.nebulostore.dfuntest.coding.messages.RecreateObjectFragmentsStatisticsMessage;

public class RecreateObjectFragmentWithSizeModule extends RecreateObjectFragmentsModule {

  private static Logger logger_ = Logger.getLogger(RecreateObjectFragmentWithSizeModule.class);

  private final Map<ObjectId, Double> receivedDataSizes_ = new HashMap<>();
  private long startTime_;
  private boolean receivedObjectsDirectly_ = false;

  @Inject
  public RecreateObjectFragmentWithSizeModule(
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    super(dispatcherQueue);
  }

  @Override
  public void getAndCalcFragments(Set<NebuloAddress> nebuloAddresses, CommAddress address,
      boolean includeMyAddress, GetModuleMode mode) {
    startTime_ = System.currentTimeMillis();
    for (NebuloAddress nebuloAddress : nebuloAddresses) {
      receivedDataSizes_.put(nebuloAddress.getObjectId(), 0.0);
    }
    super.getAndCalcFragments(nebuloAddresses, address, includeMyAddress, mode);
  }

  @Override
  protected MessageVisitor createVisitor() {
    return new RecreateWithSizeMessageVisitor();
  }

  protected class RecreateWithSizeMessageVisitor extends RecreateObjectFragmentVisitor {

    public void visit(SendObjectMessageWithSize message) {
      if (message.getMessage().getSourceAddress().equals(peerAddress_)) {
        logger_.debug("Received the desired object fragments directly " +
            "from the replicator storing them");
        receivedObjectsDirectly_ = true;
      } else {
        logger_.debug("Received " + message.getClass().getSimpleName() + " with size: " +
            message.getSize() + " and inner message: " + message.getMessage());
        Set<ObjectId> objectIds = message.getMessage().getEncryptedEntities().keySet();

        for (ObjectId objectId : objectIds) {
          receivedDataSizes_.put(objectId,
              receivedDataSizes_.get(objectId) + ((double) message.getSize()) / objectIds.size());
        }
        logger_.debug("Current received data sizes: " + receivedDataSizes_);
      }
      visit(message.getMessage());

    }

    @Override
    protected void endModuleWithSuccess(FragmentsData fragmentsData) {
      logger_.debug("Ending module with success: " + receivedDataSizes_);
      if (!receivedObjectsDirectly_) {
        outQueue_.add(new RecreateObjectFragmentsStatisticsMessage(receivedDataSizes_, System
            .currentTimeMillis() - startTime_, downloadedObjects_.size()));
      }
      super.endModuleWithSuccess(fragmentsData);
    }
  }

}
