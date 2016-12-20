package org.nebulostore.async.messages;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.replicator.core.Replicator;

/**
 * Message send to peer when he needs to update file with @objectId,
 * that he stores.
 *
 * @author szymonmatejczyk
 */
public class UpdateNebuloObjectMessage extends AsynchronousMessage {
  private static final long serialVersionUID = 1428811392987901652L;

  private transient Provider<Replicator> replicatorProvider_;

  NebuloAddress objectAddress_;

  public UpdateNebuloObjectMessage(NebuloAddress objectAddress) {
    objectAddress_ = objectAddress;
  }

  public NebuloAddress getObjectAddress() {
    return objectAddress_;
  }

  @Inject
  public void setDependencies(Provider<Replicator> replicatorProvider) {
    replicatorProvider_ = replicatorProvider;
  }

  @Override
  public JobModule getHandler() {
    return replicatorProvider_.get();
  }

  @Override
  public String toString() {
    return "{" + getClass().getSimpleName() + ": objectAddress_=" + objectAddress_ + "; " +
        super.toString() + "}";
  }

}
