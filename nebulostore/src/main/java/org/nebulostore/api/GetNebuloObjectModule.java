package org.nebulostore.api;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.replicator.messages.SendObjectsMessage;

/**
 * Job module that fetches an existing object from NebuloStore.
 *
 * @author Bolek Kulbabinski
 */
public class GetNebuloObjectModule extends
    GetModule<NebuloObject> implements ObjectGetter {
  private final MessageVisitor visitor_ = createVisitor();

  protected MessageVisitor createVisitor() {
    return new StateMachineVisitor();
  }

  @Override
  public NebuloObject awaitResult(int timeoutSec) throws NebuloException {
    return getResult(timeoutSec);
  }

  protected class StateMachineVisitor extends GetModuleVisitor {

    @Override
    public void visit(SendObjectsMessage message) {
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
    protected void endWithResult() {
      timer_.cancelTimer();
      ObjectId objectId = objectIds_.iterator().next();
      NebuloObject nebuloObject;
      try {
        nebuloObject = (NebuloObject) encryption_.decrypt(downloadedObjects_.get(objectId),
            privateKeyPeerId_);
        nebuloObject.setVersions(currentVersionsMap_.values().iterator().next());
      } catch (NebuloException exception) {
        // TODO(bolek): Error not fatal? Retry?
        endWithError(exception);
        return;
      }

      // State 3 - Finally got the files, return them;
      state_ = STATE.FILES_RECEIVED;
      endWithSuccess(nebuloObject);
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
