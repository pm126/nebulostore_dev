package org.nebulostore.crypto.session;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.appcore.modules.Module;

public class InitSessionNegotiatorMessageForwarder extends JobModule {

  private static Logger logger_ = Logger.getLogger(InitSessionNegotiatorMessageForwarder.class);

  private final Message message_;
  private Module negotiatorModule_;

  public InitSessionNegotiatorMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setNegotiatorModule(InitSessionNegotiatorModule negotiatorModule) {
    negotiatorModule_ = negotiatorModule;
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  @Override
  protected void initModule() {
    logger_.debug("Forwarding message " + message_.getClass().getSimpleName() +
        " to InitSessionNegotiatorModule.");
    negotiatorModule_.getInQueue().add(message_);
    endJobModule();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
  }

}
