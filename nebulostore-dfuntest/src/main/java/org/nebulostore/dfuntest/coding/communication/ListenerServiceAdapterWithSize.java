package org.nebulostore.dfuntest.coding.communication;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.routing.ByteListenerService;
import org.nebulostore.communication.routing.ListenerServiceAdapter;
import org.nebulostore.dfuntest.coding.communication.messages.SendObjectMessageWithSize;
import org.nebulostore.replicator.messages.SendObjectsMessage;

public class ListenerServiceAdapterWithSize extends ListenerServiceAdapter {

  @Inject
  public ListenerServiceAdapterWithSize(ByteListenerService byteListener,
      @Named("communication.routing.listening-queue") BlockingQueue<CommMessage> listeningQueue) {
    super(byteListener, listeningQueue);
  }

  @Override
  protected CommMessage deserialize(byte[] msg) throws ClassNotFoundException {
    CommMessage message = super.deserialize(msg);
    if (message instanceof SendObjectsMessage) {
      return new SendObjectMessageWithSize((SendObjectsMessage) message, msg.length);
    }

    return message;
  }
}
