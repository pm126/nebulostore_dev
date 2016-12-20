package org.nebulostore.async.synchrogroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.util.RecipientsData;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Module that refreshes synchro group cache in in the asynchronous messages context. After
 * refreshing the cache, it updates current instance's recipients set in the context and DHT.
 *
 * @author Piotr Malicki
 *
 */
public class CacheRefreshingModule extends JobModule {

  private static final long CACHE_REFRESH_TIMEOUT_MILIS = 4000;

  private static Logger logger_ = Logger.getLogger(CacheRefreshingModule.class);

  private final MessageVisitor visitor_ = new CacheRefreshingModuleVisitor();

  private final Map<KeyDHT, CommAddress> recipientsKeys_ = new HashMap<>();

  private AsyncMessagesContext context_;
  private Timer timer_;
  private CommAddress myAddress_;
  private AppKey appKey_;

  @Inject
  public void setDependencies(AsyncMessagesContext context, Timer timer, CommAddress myAddress,
      AppKey appKey) {
    context_ = context;
    timer_ = timer;
    myAddress_ = myAddress;
    appKey_ = appKey;
  }

  private enum ModuleState {
    WAITING_FOR_RESPONSES, UPDATING_DHT
  }

  protected class CacheRefreshingModuleVisitor extends MessageVisitor {

    private ModuleState state_ = ModuleState.WAITING_FOR_RESPONSES;

    public void visit(JobInitMessage message) {
      try {
        context_.waitForInitialization();
        Set<CommAddress> recipients = context_.getRecipientsData().getRecipients();
        for (CommAddress recipient : recipients) {
          networkQueue_.add(new GetDHTMessage(jobId_, recipient.toKeyDHT()));
          recipientsKeys_.put(recipient.toKeyDHT(), recipient);
        }

        if (recipients.isEmpty()) {
          finishModule();
        } else {
          logger_.info("Recipients: " + recipients);
          timer_.schedule(jobId_, CACHE_REFRESH_TIMEOUT_MILIS);
        }
      } catch (InterruptedException e) {
        logger_.warn("Interrupted while waiting for initialization of asynchronous messages " +
            "context.", e);
        finishModule();
      }
    }

    public void visit(ValueDHTMessage message) {
      logger_.info("Received ValueDHTMessage with value: " + message.getValue().getValue());
      if (state_.equals(ModuleState.WAITING_FOR_RESPONSES)) {
        if (message.getValue().getValue() instanceof InstanceMetadata) {
          InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
          CommAddress recipient = recipientsKeys_.remove(message.getKey());
          if (recipient != null) {
            context_.updateSynchroGroup(recipient, metadata.getSynchroGroup());
            if (recipientsKeys_.isEmpty()) {
              logger_.info("Cache refreshed, updating recipients in DHT");
              timer_.cancelTimer();
              updateRecipientsInDHT();
            }
          }
        } else {
          logger_.warn("Received unexpected ValueDHTMessage");
        }
      }
    }

    public void visit(TimeoutMessage message) {
      if (state_.equals(ModuleState.WAITING_FOR_RESPONSES)) {
        logger_.warn("Timeout in " + CacheRefreshingModule.class.getSimpleName());
        state_ = ModuleState.UPDATING_DHT;
        updateRecipientsInDHT();
      }
    }

    public void visit(OkDHTMessage message) {
      if (state_.equals(ModuleState.UPDATING_DHT)) {
        logger_.info("Successfully refreshed synchro group sets");
        endJobModule();
      } else {
        logger_.warn("Received unexpected " + message.getClass().getName());
      }
    }

    public void visit(ErrorDHTMessage message) {
      if (state_.equals(ModuleState.UPDATING_DHT)) {
        logger_.warn("Updating recipients set in DHT failed");
        endJobModule();
      } else {
        logger_.warn("Received unexpected " + message.getClass().getName());
      }
    }

    private void updateRecipientsInDHT() {
      state_ = ModuleState.UPDATING_DHT;
      context_.removeUnnecessarySynchroGroups();

      InstanceMetadata metadata = new InstanceMetadata(appKey_);
      RecipientsData recipientsData = context_.getRecipientsData();
      metadata.setRecipients(recipientsData.getRecipients());
      metadata.setRecipientsSetVersion(recipientsData.getRecipientsSetVersion());
      networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(metadata)));
    }

    private void finishModule() {
      timer_.cancelTimer();
      endJobModule();
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
