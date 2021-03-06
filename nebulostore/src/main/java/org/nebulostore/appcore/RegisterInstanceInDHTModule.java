package org.nebulostore.appcore;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;


/**
 * Module checks if InstanceMetadata is already in DHT. If not tries to load it from disk(todo) and
 * if it's not there, puts empty InstanceMetadata in DHT.
 *
 * Return null if new InstanceMetada was put into DHT and the downloaded metadata otherwise.
 *
 * @author szymonmatejczyk
 *
 */
public class RegisterInstanceInDHTModule extends ReturningJobModule<InstanceMetadata> {
  private static Logger logger_ = Logger.getLogger(RegisterInstanceInDHTModule.class);

  private final MessageVisitor visitor_ = new RIIDHTVisitor();

  public RegisterInstanceInDHTModule() {
  }

  /**
   * Visitor state.
   */
  private enum State {
    QUERY_DHT, WAITING_FOR_RESPONSE, PUT_DHT
  }

  private CommAddress myAddress_;
  private AppKey appKey_;

  @Inject
  public void setMyAddress(CommAddress myAddress) {
    myAddress_ = myAddress;
  }

  @Inject
  public void setMyAppKey(AppKey appKey) {
    appKey_ = appKey;
  }

  public class RIIDHTVisitor extends MessageVisitor {
    State state_ = State.QUERY_DHT;

    public void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Trying to retrieve InstanceMetadata from DHT taskId: " + jobId_);
      networkQueue_.add(new GetDHTMessage(jobId_, myAddress_.toKeyDHT()));
      state_ = State.WAITING_FOR_RESPONSE;
    }

    public void visit(ErrorDHTMessage message) {
      if (state_ == State.WAITING_FOR_RESPONSE) {
        logger_.debug("Unable to retrieve InstanceMetadata from DHT, putting new.");
        // TODO(szm): read from file if exists
        InstanceMetadata instanceMetadata = new InstanceMetadata(appKey_);
        networkQueue_
          .add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(instanceMetadata)));
        state_ = State.PUT_DHT;
      } else if (state_ == State.PUT_DHT) {
        logger_.error("Unable to put InstanceMetadata to DHT. " +
            message.getException().getMessage());
        endWithError(message.getException());
      } else {
        logger_.warn("Received unexpected ErrorDHTMessage.");
      }
    }

    public void visit(ValueDHTMessage message) {
      if (state_ == State.WAITING_FOR_RESPONSE) {
        logger_.debug("InstanceMetadata already in DHT, nothing to do.");
        endWithSuccess((InstanceMetadata) message.getValue().getValue());
      } else {
        logger_.warn("Received unexpected ValueDHTMessage");
      }
    }

    public void visit(OkDHTMessage message) {
      if (state_ == State.PUT_DHT) {
        logger_.debug("Successfuly put InstanceMetadata into DHT.");
        endWithSuccess(null);
      } else {
        logger_.warn("Received unexpected OkDHTMessage.");
      }
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
