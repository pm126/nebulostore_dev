package org.nebulostore.dfuntest.coding;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.commons.lang.SerializationUtils;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.Module;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.dfuntest.coding.messages.RecreationSentDataSizeMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectsMessage;

public class SentDataSizeCalculatorModule extends Module {

  private final BlockingQueue<Message> networkQueue_;
  private final MessageVisitor visitor_ = new CalculatorModuleVisitor();

  @Inject
  public SentDataSizeCalculatorModule(
      @Named("NetworkQueue") BlockingQueue<Message> networkQueue,
      @Named("CalculatorInQueue") BlockingQueue<Message> inQueue,
      @Named("DispatcherQueue") BlockingQueue<Message> outQueue) {
    super(inQueue, outQueue);
    networkQueue_ = networkQueue;
  }

  protected class CalculatorModuleVisitor extends MessageVisitor {

    public void visit(QueryToStoreObjectsMessage message) {
      byte[] serializedMessage = SerializationUtils.serialize(message);
      outQueue_.add(new RecreationSentDataSizeMessage(serializedMessage.length,
          message.getEncryptedEntities().keySet().size()));
      networkQueue_.add(message);
    }

    public void visit(CommMessage message) {
      networkQueue_.add(message);
    }

    public void visit(EndModuleMessage message) {
      endModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
