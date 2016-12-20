package org.nebulostore.async.synchrogroup;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.synchrogroup.messages.SynchroPeerSetUpdateJobEndedMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Module that tries to download synchro peer set of peer_ from DHT and updates this peer's set in
 * context.
 *
 * @author Piotr Malicki
 *
 */
public class SynchroPeerSetUpdateModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(SynchroPeerSetUpdateModule.class);

  private final MessageVisitor visitor_ = new SynchroPeerSetUpdateVisitor();
  private final CommAddress peer_;
  private final AsyncMessagesContext context_;
  private final BlockingQueue<Message> resultQueue_;

  public SynchroPeerSetUpdateModule(CommAddress peer, AsyncMessagesContext context,
      BlockingQueue<Message> resultQueue) {
    peer_ = peer;
    context_ = context;
    resultQueue_ = resultQueue;
  }

  protected class SynchroPeerSetUpdateVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) {
      logger_.debug("Starting " + SynchroPeerSetUpdateModule.class.getName() + " for peer " +
          peer_);
      try {
        context_.waitForInitialization();
        networkQueue_.add(new GetDHTMessage(jobId_, peer_.toKeyDHT()));
      } catch (InterruptedException e) {
        logger_.warn("Interrupted while waiting for initialization of asynchronous messages " +
            "context.", e);
        endJobModule();
      }
    }

    public void visit(ValueDHTMessage message) {
      if (message.getKey().equals(peer_.toKeyDHT())) {
        if (message.getValue().getValue() instanceof InstanceMetadata) {
          InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
          logger_.debug("Downloaded metadata of peer: " + peer_ + ", synchro group: " +
              metadata.getSynchroGroup());
          context_.updateSynchroGroup(peer_, metadata.getSynchroGroup());
          resultQueue_.add(new SynchroPeerSetUpdateJobEndedMessage(jobId_));
        } else {
          logger_.warn("Received ValueDHTMessage with a wrong key.");
        }
      } else {
        logger_.warn("Wrong type of value in ValueDHTMessage.");
      }
      endJobModule();
    }

    public void visit(ErrorDHTMessage message) {
      logger_.warn("Could not download InstanceMetadata for address " + peer_);
      resultQueue_.add(new SynchroPeerSetUpdateJobEndedMessage(jobId_));
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
