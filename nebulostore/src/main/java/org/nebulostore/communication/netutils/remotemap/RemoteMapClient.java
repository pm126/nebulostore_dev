package org.nebulostore.communication.netutils.remotemap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * @author Grzegorz Milka
 */
public class RemoteMapClient implements RemoteMap {
  private static final Logger LOGGER = Logger.getLogger(RemoteMapClient.class);
  private static final int N_TRIES = 2;
  private final InetSocketAddress serverAddress_;

  public RemoteMapClient(InetSocketAddress serverAddress) {
    serverAddress_ = serverAddress;
  }

  @Override
  public Serializable get(int type, String key) throws IOException {
    for (int i = 1; i < N_TRIES; ++i) {
      try {
        return getOnce(type, key);
      } catch (IOException e) {
        LOGGER.trace(String.format("get(%d, %s)", type, key), e);
      }
    }
    return getOnce(type, key);
  }

  @Override
  public void performTransaction(int type, String key, Transaction transaction)
      throws IOException {
    LOGGER.info(String.format("performTransaction(%d,%s, %s)", type, key, transaction));
    try (Socket socket = new Socket(serverAddress_.getAddress(), serverAddress_.getPort())) {
      LOGGER.debug("Socket created: " + socket.getLocalPort());
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      try {
        oos.write(TRAN_ID);
        oos.write(type);
        oos.writeObject(key);
        oos.writeObject(transaction);
        LOGGER.debug("Written object " + transaction);
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        int res = ois.readInt();
        if (res == SUCCESS_ID) {
          LOGGER.debug("Transaction finished successfully");
        } else if (res == FAIL_ID) {
          throw new IOException("Transaction failed");
        }
      } finally {
        oos.flush();
        oos.close();
      }
    }
  }

  @Override
  public void put(int type, String key, Serializable value) throws IOException {
    LOGGER.info(String.format("put(%d,%s, %s)", type, key, value));
    try (Socket socket = new Socket(serverAddress_.getAddress(), serverAddress_.getPort())) {
      LOGGER.debug("Socket created: " + socket.getLocalPort());
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      try {
        oos.write(PUT_ID);
        oos.write(type);
        oos.writeObject(key);
        oos.writeObject(value);
        LOGGER.info("Written object: " + "(" + key + ", " + value + ")");
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        int res = ois.readInt();
        if (res == SUCCESS_ID) {
          LOGGER.debug("Put operation finished successfully");
        } else if (res == FAIL_ID) {
          throw new IOException("Put operation failed");
        }
      } finally {
        oos.flush();
        oos.close();
      }
    }
  }

  private Serializable getOnce(int type, Serializable key) throws IOException {
    LOGGER.trace(String.format("get(%d, %s)", type, key));
    try (Socket socket = new Socket(serverAddress_.getAddress(), serverAddress_.getPort())) {
      LOGGER.debug("Socket created: " + socket.getLocalPort());
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      oos.write(GET_ID);
      oos.write(type);
      oos.writeObject(key);
      LOGGER.debug("Before creating a stream");

      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      LOGGER.debug("Object stream created");
      try {
        Serializable value = (Serializable) ois.readObject();
        LOGGER.info(String.format("get(%d, %s): %s", type, key, value));
        return value;
      } catch (ClassNotFoundException e) {
        LOGGER.warn(String.format("get(%d,%s): null", type, key), e);
        return null;
      }
    } catch (IOException e) {
      throw e;
    }
  }

}
