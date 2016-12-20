package org.nebulostore.crypto;

import java.io.Serializable;
import java.security.Key;
import java.security.KeyPair;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.keys.DHTKeyHandler;
import org.nebulostore.crypto.keys.FileKeySource;
import org.nebulostore.crypto.keys.KeyHandler;
import org.nebulostore.crypto.keys.KeySource;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 */
public class RSABasedEncryptionAPI extends EncryptionAPI {

  private static final Logger LOGGER = Logger.getLogger(RSABasedEncryptionAPI.class);
  private static final Long REFRESH_PERIOD_MILIS = 60000L;

  private final ConcurrentMap<String, KeyHandler> keyHandlers_;
  private final ConcurrentMap<String, Key> keys_ = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> lastRefreshTimes_ = new ConcurrentHashMap<>();
  private final CommAddress peerAddress_;
  private final BlockingQueue<Message> dispatcherQueue_;

  @Inject
  public RSABasedEncryptionAPI(CommAddress peerAddress,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    keyHandlers_ = new ConcurrentHashMap<String, KeyHandler>();
    peerAddress_ = peerAddress;
    dispatcherQueue_ = dispatcherQueue;
  }

  @Override
  public EncryptedObject encrypt(Serializable object, String keyId) throws CryptoException {
    LOGGER.debug(String.format("encrypt  %s", keyId));
    return CryptoUtils.encryptObject(object, getKey(keyId));
  }

  @Override
  public Object decrypt(EncryptedObject cipher, String keyId) throws CryptoException {
    LOGGER.debug(String.format("decrypt  %s", keyId));
    return CryptoUtils.decryptObject(cipher, getKey(keyId));
  }

  @Override
  public EncryptedObject encryptWithSessionKey(Serializable object, SecretKey key)
      throws CryptoException {
    LOGGER.debug("encrypt session key");
    return CryptoUtils.encryptObjectWithSessionKey(object, key);
  }

  @Override
  public Object decryptWithSessionKey(EncryptedObject cipher, SecretKey key) throws CryptoException {
    LOGGER.debug("decrypt session key");
    return CryptoUtils.decryptObjectWithSessionKey(cipher, key);
  }

  @Override
  public void load(String keyId, KeySource keySource, boolean saveInDHT) throws CryptoException {
    LOGGER.debug(String.format("load %s %s %s", keyId, keySource, saveInDHT));
    KeyHandler keyHandler = keySource.getKeyHandler();
    if (saveInDHT) {
      DHTKeyHandler dhtKeyHandler = new DHTKeyHandler(peerAddress_, dispatcherQueue_);
      dhtKeyHandler.save(keyHandler.load());
      keyHandler = dhtKeyHandler;
    }
    keyHandlers_.put(keyId, keyHandler);
  }

  /**
   * @return Pair<Private Key Id, Public Key Id>
   */
  @Override
  public Pair<String, String> generatePublicPrivateKey() throws CryptoException {
    LOGGER.debug(String.format("generatePublicPrivateKey"));
    KeyPair keyPair = CryptoUtils.generateKeyPair();
    String privateKeyId = CryptoUtils.getRandomString();
    String publicKeyId = CryptoUtils.getRandomString();
    String privateKeyPath = CryptoUtils.saveKeyOnDisk(keyPair.getPrivate(), privateKeyId);
    String publicKeyPath = CryptoUtils.saveKeyOnDisk(keyPair.getPublic(), publicKeyId);
    load(privateKeyId, new FileKeySource(privateKeyPath, KeyType.PRIVATE), !STORE_IN_DHT);
    load(publicKeyId, new FileKeySource(publicKeyPath, KeyType.PUBLIC), !STORE_IN_DHT);
    return new Pair<String, String>(privateKeyId, publicKeyId);
  }

  private Key getKey(String keyId) throws CryptoException {
    if (keys_.containsKey(keyId) && lastRefreshTimes_.containsKey(keyId) &&
        lastRefreshTimes_.get(keyId) + REFRESH_PERIOD_MILIS >= System.currentTimeMillis()) {
      return keys_.get(keyId);
    } else {
      Key key = keyHandlers_.get(keyId).load();
      keys_.put(keyId, key);
      lastRefreshTimes_.put(keyId, System.currentTimeMillis());
      return key;
    }
  }
}
