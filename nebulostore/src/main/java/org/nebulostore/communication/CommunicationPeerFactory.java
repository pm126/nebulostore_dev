package org.nebulostore.communication;

import java.util.concurrent.BlockingQueue;

import com.google.inject.assistedinject.Assisted;

import org.nebulostore.appcore.messaging.Message;

/**
 * Factory for AssistedInject of CommunicationPeer.
 *
 * @author Grzegorz Milka
 */
public interface CommunicationPeerFactory {
  Runnable newCommunicationPeer(
      @Assisted("CommunicationPeerInQueue") BlockingQueue<Message> inQueue,
      @Assisted("CommunicationPeerOutQueue") BlockingQueue<Message> outQueue);
}
