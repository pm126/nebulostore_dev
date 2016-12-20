package org.nebulostore.crypto.session;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.KeyAgreement;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.dh.DiffieHellmanInitPackage;
import org.nebulostore.crypto.dh.DiffieHellmanProtocol;
import org.nebulostore.crypto.dh.DiffieHellmanResponsePackage;
import org.nebulostore.crypto.session.message.GetSessionKeyMessage;
import org.nebulostore.crypto.session.message.GetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.InitSessionEndMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
import org.nebulostore.crypto.session.message.InitSessionErrorMessage;
import org.nebulostore.crypto.session.message.InitSessionMessage;
import org.nebulostore.crypto.session.message.InitSessionResponseMessage;
import org.nebulostore.crypto.session.message.LocalInitSessionMessage;
import org.nebulostore.crypto.session.message.SessionCryptoMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 */
public class InitSessionNegotiatorModule extends JobModule {

  private static final Logger LOGGER = Logger.getLogger(InitSessionNegotiatorModule.class);
  private static final int TIMEOUT_MILIS = 10000;

  private enum ErrorNotificationMethod { NONE, LOCAL, REMOTE, ALL };
  private enum NegotiatorState { INITIALIZATION, RESPONDING }

  private final InitSessionNegotiatorModuleVisitor sessionNegotiatorModuleVisitor_ =
      new InitSessionNegotiatorModuleVisitor();
  private final InitSessionContext initSessionContext_;

  private final CommAddress myAddress_;
  private final NetworkMonitor networkMonitor_;
  private final EncryptionAPI encryptionAPI_;
  private final String privateKeyPeerId_;
  private final Timer timer_;

  private final Map<String, NegotiatorState> states_ = new HashMap<>();
  private final Map<String, String> localSourceJobIds_ = new HashMap<>();
  private final Map<String, String> remoteSourceJobIds_ = new HashMap<>();
  private final Map<String, CommAddress> peerAddresses_ = new HashMap<>();
  private final Map<String, String> timerTaskIds_ = new HashMap<>();

  @Inject
  public InitSessionNegotiatorModule(
      CommAddress myAddress,
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      @Named("PrivateKeyPeerId") String privateKeyPeerId,
      InitSessionContext initSessionContext,
      Timer timer) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
    encryptionAPI_ = encryptionAPI;
    privateKeyPeerId_ = privateKeyPeerId;
    initSessionContext_ = initSessionContext;
    timer_ = timer;
  }

  protected class InitSessionNegotiatorModuleVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {

    }

    public void visit(LocalInitSessionMessage message) {
      LOGGER.debug("Process LocalInitSessionMessage: " + message);

      String sessionId = null;
      CommAddress peerAddress = message.getPeerAddress();
      try {
        sessionId = initSessionContext_.tryAllocFreeSlot(new InitSessionObject(
            peerAddress, message.getLocalSourceJobId(), message.getData()));
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.LOCAL, peerAddress, message.getLocalSourceJobId(),
            null, null, message.getData());
        return;
      }
      states_.put(sessionId, NegotiatorState.INITIALIZATION);
      localSourceJobIds_.put(sessionId, message.getLocalSourceJobId());
      peerAddresses_.put(sessionId, peerAddress);
      LOGGER.debug("Acquired sessionId: " + sessionId);
      String peerKeyId = networkMonitor_.getPeerPublicKeyId(peerAddress);
      try {
        Pair<KeyAgreement, DiffieHellmanInitPackage> firstStep =
            DiffieHellmanProtocol.firstStepDHKeyAgreement();
        EncryptedObject encryptedData = encryptionAPI_.encrypt(firstStep.getSecond(), peerKeyId);
        initSessionContext_.tryGetInitSessionObject(sessionId).setKeyAgreement(
            firstStep.getFirst());
        Message initSessionMessage = new InitSessionMessage(myAddress_, peerAddress, sessionId,
            getJobId(), encryptedData);
        timerTaskIds_.put(sessionId, timer_.schedule(jobId_, TIMEOUT_MILIS, sessionId, false));
        networkQueue_.add(initSessionMessage);
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.LOCAL, sessionId, message.getData());
      }
    }

    public void visit(InitSessionMessage message) {
      LOGGER.debug("Process InitSessionMessage: " + message);
      states_.put(message.getSessionId(), NegotiatorState.RESPONDING);
      CommAddress peerAddress = message.getSourceAddress();
      String remoteSourceJobId = message.getSourceJobId();
      String sessionId = message.getSessionId();
      if (!peerAddress.equals(myAddress_)) {
        try {
          initSessionContext_.allocFreeSlot(sessionId, new InitSessionObject(peerAddress));
        } catch (SessionRuntimeException e) {
          endWithError(e, ErrorNotificationMethod.REMOTE, peerAddress, null,
              message.getSourceJobId(), message.getSessionId());
          return;
        }
      }
      try {
        LOGGER.debug("Before decrypting the diffie hellman package");
        DiffieHellmanInitPackage diffieHellmanInitPackage = (DiffieHellmanInitPackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), privateKeyPeerId_);
        LOGGER.debug("Diffie Hellman package decrypted");
        Pair<KeyAgreement, DiffieHellmanResponsePackage> secondStep =
            DiffieHellmanProtocol.secondStepDHKeyAgreement(diffieHellmanInitPackage);
        LOGGER.debug("Second step calculated");
        initSessionContext_.tryGetInitSessionObject(sessionId).setSessionKey(
            DiffieHellmanProtocol.fourthStepDHKeyAgreement(secondStep.getFirst()));
        LOGGER.debug("Init session object got");
        String peerKeyId = networkMonitor_.getPeerPublicKeyId(peerAddress);
        LOGGER.debug("Public key id got");
        EncryptedObject encryptedData = encryptionAPI_.encrypt(secondStep.getSecond(), peerKeyId);
        LOGGER.debug("Data for response encrypted");
        Message initSessionResponseMessage = new InitSessionResponseMessage(remoteSourceJobId,
            myAddress_, peerAddress, sessionId, getJobId(), encryptedData);
        networkQueue_.add(initSessionResponseMessage);
        LOGGER.debug("Added response to the network queue");
        endWithSuccess(sessionId);
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.REMOTE, sessionId);
      }
    }

    public void visit(InitSessionResponseMessage message) {
      LOGGER.debug("Process InitSessionResponseMessage: " + message);
      if (!states_.containsKey(message.getSessionId())) {
        LOGGER.warn("Received " + message.getClass().getSimpleName() + " with an unknown " +
            "session id");
        endWithError(new CryptoException("The session with gived id has not been started"),
            ErrorNotificationMethod.REMOTE, message.getSourceAddress(), null,
            message.getSourceJobId(), message.getSessionId());
        return;
      }

      remoteSourceJobIds_.put(message.getSessionId(), message.getSourceJobId());
      InitSessionObject initSessionObject = null;
      try {
        initSessionObject = initSessionContext_.tryGetInitSessionObject(message.getSessionId());
      } catch (SessionRuntimeException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.ALL, message.getSessionId());
        return;
      }
      try {
        timer_.cancelTask(timerTaskIds_.remove(message.getSessionId()));
        LOGGER.debug("Before decrypting the package");
        DiffieHellmanResponsePackage diffieHellmanResponsePackage = (DiffieHellmanResponsePackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), privateKeyPeerId_);
        LOGGER.debug("Package decrypted");
        KeyAgreement keyAgreement = DiffieHellmanProtocol.thirdStepDHKeyAgreement(
            initSessionObject.getKeyAgreement(), diffieHellmanResponsePackage);
        LOGGER.debug("Third step calculated");
        initSessionObject.setSessionKey(
            DiffieHellmanProtocol.fourthStepDHKeyAgreement(keyAgreement));
        LOGGER.debug("Fourth step calculated");

        InitSessionEndMessage initSessionEndMessage =
            new InitSessionEndMessage(initSessionObject, getJobId());
        outQueue_.add(initSessionEndMessage);
        LOGGER.debug("Response: " + initSessionEndMessage + " added to the out queue");
        if (message.getSourceAddress().equals(myAddress_)) {
          endWithSuccess(message.getSessionId());
        } else {
          endWithSuccessAndClear(message.getSessionId());
        }
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.ALL, message.getSessionId(),
            initSessionObject.getData());
      }
    }

    public void visit(GetSessionKeyMessage message) {
      LOGGER.debug("Process GetSessionKeyMessage: " + message);
      CommAddress peerAddress = message.getPeerAddress();
      String localSourceJobId = message.getSourceJobId();
      String sessionId = message.getSessionId();
      InitSessionObject initSessionObject = null;
      try {
        initSessionObject = initSessionContext_.tryGetInitSessionObject(sessionId);
        if (initSessionObject.getSessionKey() == null) {
          throw new SessionRuntimeException("SessionKey null");
        }
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.LOCAL, peerAddress, message.getSourceJobId(), null,
            sessionId);
        return;
      }
      outQueue_.add(new GetSessionKeyResponseMessage(localSourceJobId,
          peerAddress, initSessionObject.getSessionKey(), sessionId));
      endWithSuccessAndClear(sessionId);
    }

    public void visit(InitSessionErrorMessage message) {
      LOGGER.debug("Process InitSessionErrorMessage: " + message);
      endWithErrorAndClear(new SessionRuntimeException(message.getErrorMessage()),
          ErrorNotificationMethod.LOCAL, message.getSessionId(), tryGetSessionDataObject(
          message.getSessionId()));
    }

    public void visit(ErrorCommMessage message) {
      LOGGER.warn("Message " + message.getMessage() + " has not been sent");
      SessionCryptoMessage msg = (SessionCryptoMessage) message.getMessage();
      if (msg != null) {
        switch (states_.get(msg.getSessionId())) {
          case INITIALIZATION:
            timer_.cancelTask(timerTaskIds_.remove(msg.getSessionId()));
            endWithErrorAndClear(message.getNetworkException(), ErrorNotificationMethod.LOCAL,
                msg.getSessionId(), tryGetSessionDataObject(msg.getSessionId()));
            break;
          case RESPONDING:
            endWithErrorAndClear(message.getNetworkException(), ErrorNotificationMethod.REMOTE,
                msg.getSessionId());
            break;
          default:
        }
      }
    }

    public void visit(TimeoutMessage message) {
      if (peerAddresses_.containsKey(message.getMessageContent())) {
        LOGGER.warn("Timeout reached, cancelling the request for session id: " +
            message.getMessageContent());
        if (states_.get(message.getMessageContent()).equals(NegotiatorState.INITIALIZATION)) {
          timer_.cancelTask(timerTaskIds_.remove(message.getMessageContent()));
          endWithErrorAndClear(new NebuloException(
              "Timeout reached while waiting for the response"),
              ErrorNotificationMethod.LOCAL, (String) message.getMessageContent(),
              tryGetSessionDataObject((String) message.getMessageContent()));
        }
      }
    }

    private void endWithErrorAndClear(Throwable e, ErrorNotificationMethod method,
        String sessionId) {
      endWithErrorAndClear(e, method, sessionId, null);
    }

    private void endWithErrorAndClear(Throwable e, ErrorNotificationMethod method,
        String sessionId, Serializable data) {
      try {
        initSessionContext_.tryRemoveInitSessionObject(sessionId);
        states_.remove(sessionId);
      } catch (SessionRuntimeException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
      endWithError(e, method, peerAddresses_.remove(sessionId),
          localSourceJobIds_.remove(sessionId), remoteSourceJobIds_.remove(sessionId),
          sessionId, data);
    }

    private void endWithError(Throwable e, ErrorNotificationMethod method,
        CommAddress peerAddress, String localSourceJobId, String remoteSourceJobId,
        String sessionId) {
      endWithError(e, method, peerAddress, localSourceJobId, remoteSourceJobId, sessionId, null);

    }

    private void endWithError(Throwable e, ErrorNotificationMethod method,
        CommAddress peerAddress, String localSourceJobId, String remoteSourceJobId,
        String sessionId, Serializable data) {
      LOGGER.debug(e.getMessage(), e);
      switch (method) {
        case NONE:
          break;
        case LOCAL:
          localErrorNotify(localSourceJobId, peerAddress, e, data);
          break;
        case REMOTE:
          remoteErrorNotify(remoteSourceJobId, peerAddress, sessionId, e);
          break;
        case ALL:
        default:
          localErrorNotify(localSourceJobId, peerAddress, e, data);
          remoteErrorNotify(remoteSourceJobId, peerAddress, sessionId, e);
          break;
      }
    }

    private void remoteErrorNotify(String destinationJobId, CommAddress peerAddress,
        String sessionId, Throwable e) {
      networkQueue_.add(new InitSessionErrorMessage(destinationJobId, myAddress_,
          peerAddress, sessionId, e.getMessage()));
    }

    private void localErrorNotify(String destinationJobId, CommAddress peerAddress, Throwable e,
        Serializable data) {
      outQueue_.add(new InitSessionEndWithErrorMessage(
          destinationJobId, e.getMessage(), peerAddress, data));
    }

    private void endWithSuccessAndClear(String sessionId) {
      try {
        initSessionContext_.tryRemoveInitSessionObject(sessionId);
      } catch (SessionRuntimeException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
      states_.remove(sessionId);
      endWithSuccess(sessionId);
    }

    private void endWithSuccess(String sessionId) {
      peerAddresses_.remove(sessionId);
      localSourceJobIds_.remove(sessionId);
      remoteSourceJobIds_.remove(sessionId);
    }

    private Serializable tryGetSessionDataObject(String sessionId) {
      InitSessionObject initSessionObject = null;
      try {
        initSessionObject = initSessionContext_.tryGetInitSessionObject(sessionId);
      } catch (SessionRuntimeException e) {
        LOGGER.warn("SessionRuntimeException when trying to get init session object.", e);
      }
      if (initSessionObject != null) {
        return initSessionObject.getData();
      }
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(sessionNegotiatorModuleVisitor_);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
