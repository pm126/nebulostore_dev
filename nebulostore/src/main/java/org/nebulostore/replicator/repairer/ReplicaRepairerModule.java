package org.nebulostore.replicator.repairer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetModule.GetModuleMode;
import org.nebulostore.api.RecreateObjectFragmentsModule;
import org.nebulostore.api.RecreateObjectFragmentsModule.FragmentsData;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.PartialObjectWriter;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.ReplicatorData;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.repairer.messages.RepairFailedMessage;
import org.nebulostore.replicator.repairer.messages.RepairSuccessfulMessage;

/**
 * @author Piotr Malicki
 */
public class ReplicaRepairerModule extends JobModule {

  private static final int FETCH_OBJECT_TIMEOUT_MILIS = 5000;
  private static final int WRITE_OBJECT_TIMEOUT_MILIS = 5000;
  private static final int WRITE_FINISH_TIMEOUT_MILIS = 1000;

  private static Logger logger_ = Logger.getLogger(ReplicaRepairerModule.class);

  private final AppKey appKey_;
  private final List<CommAddress> currentReplicators_;
  private final ReplicatorData newReplicator_;
  private final Collection<ObjectId> objectIds_;
  private final Provider<PartialObjectWriter> writeModuleProvider_;
  private final Provider<RecreateObjectFragmentsModule> recreateObjectModuleProvider_;

  protected MessageVisitor visitor_;

  @Inject
  public ReplicaRepairerModule(
      @Assisted("CurrentReplicators") List<CommAddress> currentReplicators,
      @Assisted("NewReplicator") ReplicatorData newReplicator,
      @Assisted("ObjectIds") Collection<ObjectId> objectIds,
      Provider<PartialObjectWriter> writeModuleProvider, AppKey appKey,
      Provider<RecreateObjectFragmentsModule> recreateObjectModuleProvider) {
    currentReplicators_ = currentReplicators;
    newReplicator_ = newReplicator;
    objectIds_ = objectIds;
    writeModuleProvider_ = writeModuleProvider;
    appKey_ = appKey;
    recreateObjectModuleProvider_ = recreateObjectModuleProvider;
    visitor_ = new ReplicaRepairerVisitor();
  }

  protected class ReplicaRepairerVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Starting a repair with parameters:" +
          "\ncurrent replicators: " + currentReplicators_ +
          "\nnew replicator: " + newReplicator_ +
          "\nobjectIds: " + objectIds_);

      if (objectIds_.isEmpty()) {
        logger_.info("No objects to repair. Ending the module");
        endWithSuccessNotification();
        return;
      }

      Set<NebuloAddress> addresses = new HashSet<>();
      for (ObjectId objectId : objectIds_) {
        addresses.add(new NebuloAddress(appKey_, objectId));
      }

      RecreateObjectFragmentsModule recreateModule = recreateObjectModuleProvider_.get();
      recreateModule.getAndCalcFragments(addresses,
          currentReplicators_.get(newReplicator_.getSequentialNumber()), true,
          GetModuleMode.STRICT);

      FragmentsData fragmentsData;
      try {
        fragmentsData = recreateModule.getResult(FETCH_OBJECT_TIMEOUT_MILIS);
        logger_.debug("Downloaded fragments: " + fragmentsData + " by module of jobId: " +
            recreateModule.getJobId());
      } catch (NebuloException e) {
        endWithErrorNotification(new NebuloException("Could not download some of the fragments."));
        return;
      }

      try {
        writeObjects(fragmentsData);
      } catch (NebuloException e) {
        endWithErrorNotification(new NebuloException("Write module failed, transaction aborted."));
        return;
      }
      endWithSuccessNotification();
    }

    private void writeObjects(FragmentsData fragmentsData) throws NebuloException {
      Map<ObjectId, Map<CommAddress, EncryptedObject>> fragmentsMaps = new HashMap<>();
      for (Entry<ObjectId, EncryptedObject> entry : fragmentsData.fragmentsMap_.entrySet()) {
        ObjectId objectId = entry.getKey();
        Map<CommAddress, EncryptedObject> fragmentsMap = new HashMap<>();
        fragmentsMap.put(newReplicator_.getContract().getPeer(), entry.getValue());
        fragmentsMaps.put(objectId, fragmentsMap);
      }

      PartialObjectWriter writeModule = writeModuleProvider_.get();
      writeModule.writeObjects(fragmentsMaps, fragmentsData.versionsMap_,
          1, fragmentsData.objectsSizes_);
      try {
        writeModule.getSemiResult(WRITE_OBJECT_TIMEOUT_MILIS);
      } catch (NebuloException e) {
        writeModule.setAnswer(TransactionAnswer.ABORT);
        throw e;
      }

      writeModule.setAnswer(TransactionAnswer.COMMIT);

      try {
        writeModule.awaitResult(WRITE_FINISH_TIMEOUT_MILIS);
      } catch (NebuloException e) {
        logger_.warn("Error while waiting for finishing write module: " + writeModule, e);
      }

      logger_.debug("Repair ended successfully.");
    }

    private void endWithErrorNotification(Exception exception) {
      outQueue_.add(new RepairFailedMessage(jobId_, exception,
          newReplicator_.getContract().getPeer()));
      endJobModule();
    }

    private void endWithSuccessNotification() {
      outQueue_.add(new RepairSuccessfulMessage(jobId_, newReplicator_.getContract().getPeer()));
      endJobModule();
    }

  }



  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
