package org.nebulostore.dfuntest.comm;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

public class CommPerfTestHelperMessageForwarder extends JobModule {

  private JobModule helper_;
  private final Message message_;

  public CommPerfTestHelperMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setHelper(CommPerfTestHelperModule helper) {
    helper_ = helper;
  }

  @Override
  protected void initModule() {
    helper_.getInQueue().add(message_);
    endJobModule();
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {

  }

}
