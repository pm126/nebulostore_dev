package org.nebulostore.dfuntest.coding;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

public class CodingTestHelperMessageForwarder extends JobModule {

  private CodingTestHelperModule helper_;
  private final Message message_;

  public CodingTestHelperMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setCodingHelper(CodingTestHelperModule helper) {
    helper_ = helper;
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  @Override
  protected void initModule() {
    helper_.getInQueue().add(message_);
    endJobModule();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
  }

}
