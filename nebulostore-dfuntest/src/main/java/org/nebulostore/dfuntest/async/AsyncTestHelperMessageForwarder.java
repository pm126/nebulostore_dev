package org.nebulostore.dfuntest.async;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

public class AsyncTestHelperMessageForwarder extends JobModule {

  private final Message message_;
  private AsyncTestHelperModule helper_;

  public AsyncTestHelperMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setDependencies(AsyncTestHelperModule helper) {
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
  };

  @Override
  protected void processMessage(Message message) throws NebuloException {

  }

}
