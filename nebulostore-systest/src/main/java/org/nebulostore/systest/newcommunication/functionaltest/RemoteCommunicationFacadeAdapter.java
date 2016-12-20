package org.nebulostore.systest.newcommunication.functionaltest;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import org.nebulostore.communication.messages.CommMessage;

/**
 * Remote interface adapter to {@link CommunicationFacade}.
 *
 * @author Grzegorz Milka
 *
 */
public interface RemoteCommunicationFacadeAdapter extends Remote {
  void sendMessage(CommMessage message) throws RemoteException;
  void startUp() throws IOException;
  void shutDown() throws InterruptedException, RemoteException;
}
