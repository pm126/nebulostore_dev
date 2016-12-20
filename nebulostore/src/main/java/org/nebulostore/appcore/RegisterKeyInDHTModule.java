package org.nebulostore.appcore;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

public class RegisterKeyInDHTModule extends ReturningJobModule<Metadata> {

  private static Logger logger_ = Logger.getLogger(RegisterKeyInDHTModule.class);

  private final AppKey appKey_;
  private final MessageVisitor visitor_ = new RegisterKeyMessageVisitor();
  private State state_ = State.GET_KEY;

  private enum State { GET_KEY, PUT_KEY }

  public RegisterKeyInDHTModule(BlockingQueue<Message> dispatcherQueue, AppKey appKey) {
    super();
    setDispatcherQueue(dispatcherQueue);
    appKey_ = appKey;
  }

  protected class RegisterKeyMessageVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(appKey_.getKey())));
    }

    public void visit(ValueDHTMessage message) {
      //FIXME jakieś sprawdzenia?
      endWithSuccess((Metadata) message.getValue().getValue());
    }

    public void visit(ErrorDHTMessage message) {
      if (state_.equals(State.GET_KEY)) {
        state_ = State.PUT_KEY;
        networkQueue_.add(new PutDHTMessage(jobId_, new KeyDHT(appKey_.getKey()),
            new ValueDHT(new Metadata(appKey_, new ContractList()))));
      } else {
        logger_.warn("Unable to put key in DHT", message.getException());
        endWithError(message.getException());
      }
    }

    public void visit(OkDHTMessage message) {
      if (state_.equals(State.PUT_KEY)) {
        endWithSuccess(null);
      }
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
