package org.nebulostore.broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.broker.ContractsSelectionAlgorithm.ImproveGroupResponse;
import org.nebulostore.broker.ContractsSelectionAlgorithm.OfferResponse;
import org.nebulostore.broker.messages.AsyncBreakContractMessage;
import org.nebulostore.broker.messages.BreakContractMessage;
import org.nebulostore.broker.messages.CheckContractMessage;
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.ImproveContractsMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.broker.messages.WriteFinishedMessage;
import org.nebulostore.broker.messages.WritePermissionRejectionMessage;
import org.nebulostore.broker.messages.WritePermissionRequestMessage;
import org.nebulostore.broker.messages.WritePermissionResponseMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.session.message.GetSessionKeyMessage;
import org.nebulostore.crypto.session.message.GetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.InitSessionEndMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
import org.nebulostore.crypto.session.message.LocalInitSessionMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.GetPeersStatisticsModule;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.networkmonitor.StatisticsList;
import org.nebulostore.replicator.repairer.ReplicaRepairerModule;
import org.nebulostore.replicator.repairer.ReplicaRepairerModuleFactory;
import org.nebulostore.replicator.repairer.messages.RepairFailedMessage;
import org.nebulostore.replicator.repairer.messages.RepairSuccessfulMessage;
import org.nebulostore.timer.MessageGenerator;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module that initializes Broker and provides methods shared by modules in broker package.
 *
 * @author bolek, szymonmatejczyk
 */
public class ValuationBasedBroker extends Broker {
  protected static Logger logger_ = LoggerFactory.getLogger(ValuationBasedBroker.class);
  private static final String CONFIGURATION_PREFIX = "broker.";

  private static final String GROUP_UPDATE_TIMEOUT_NAME =
      "replicator.replication-group-update-timeout";
  private static final String CONTRACTS_IMPROVEMENT_PERIOD_NAME = CONFIGURATION_PREFIX +
      "contracts-improvement-period-sec";
  private static final String CONTRACTS_IMPROVEMENT_DELAY_NAME = CONFIGURATION_PREFIX +
      "contracts-improvement-delay-sec";
  private static final String CONTRACT_SIZE_NAME = CONFIGURATION_PREFIX +
      "default-contract-size-kb";
  private static final String MAX_CONTRACTS_MULTIPLICITY_NAME = CONFIGURATION_PREFIX +
      "max-contracts-multiplicity";
  private static final String SPACE_CONTRIBUTED_NAME = CONFIGURATION_PREFIX +
      "space-contributed-kb";

  private static final int REPAIRER_MODULE_TIMEOUT = 10000;
  private static final int STATS_REFRESHER_EXECUTOR_THREAD_POOL_SIZE = 1;
  private static final int STATS_REFRESHING_PERIOD_SEC = 30;
  private static final int STATS_REFRESHING_INITIAL_DELAY_SEC = 0;
  private static final int STATS_REFRESHING_PEERS_NUMBER = 5;
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 10;

  private final Map<String, SecretKey> sessionKeys_ = new HashMap<>();
  private final Map<String, EncryptedObject> contractOffer_ = new HashMap<>();
  private final Map<CommAddress, GroupUpdateData> groupUpdatesMap_ = new HashMap<>();
  private final Set<CommAddress> addressLocks_ = new HashSet<>();
  private final Map<ReplicationGroup, GroupLockState> groupLocks_ = new HashMap<>();
  private final Map<ReplicationGroup, Set<String>> waitingWriteModules_ = new HashMap<>();
  private final Map<ReplicationGroup, Set<String>> runningWriteModules_ = new HashMap<>();

  private ReplicaRepairerModuleFactory replicaRepairerFactory_;
  private final ScheduledExecutorService executor_ = Executors
      .newScheduledThreadPool(STATS_REFRESHER_EXECUTOR_THREAD_POOL_SIZE);

  public ValuationBasedBroker() {
    visitor_ = new ValuationBrokerVisitor();
  }

  protected ValuationBasedBroker(String jobId) {
    this();
    jobId_ = jobId;
  }

  @Inject
  public void setDependencies(@Named(GROUP_UPDATE_TIMEOUT_NAME) int replicationGroupUpdateTimeout,
      NetworkMonitor networkMonitor,
      @Named(CONTRACTS_IMPROVEMENT_PERIOD_NAME) int contractImprovementPeriod,
      @Named(CONTRACTS_IMPROVEMENT_DELAY_NAME) int contractImprovementDelay,
      @Named(CONTRACT_SIZE_NAME) int defaultContractSizeKb,
      ContractsSelectionAlgorithm contractsSelectionAlgorithm,
      @Named(MAX_CONTRACTS_MULTIPLICITY_NAME) int maxContractsMultiplicity,
      @Named(SPACE_CONTRIBUTED_NAME) int spaceContributedKb, Timer timer,
      ReplicaRepairerModuleFactory replicaRepairerFactory, Provider<Timer> repairerTimerProvider) {
    replicationGroupUpdateTimeout_ = replicationGroupUpdateTimeout;
    networkMonitor_ = networkMonitor;
    contractImprovementPeriod_ = contractImprovementPeriod;
    contractImprovementDelay_ = contractImprovementDelay;
    defaultContractSizeKb_ = defaultContractSizeKb;
    contractsSelectionAlgorithm_ = contractsSelectionAlgorithm;
    maxContractsMultiplicity_ = maxContractsMultiplicity;
    spaceContributedKb_ = spaceContributedKb;
    timer_ = timer;
    replicaRepairerFactory_ = replicaRepairerFactory;
    repairerTimerProvider_ = repairerTimerProvider;
  }

  // Injected constants.
  private int replicationGroupUpdateTimeout_;
  private int contractImprovementPeriod_;
  private int contractImprovementDelay_;
  private int defaultContractSizeKb_;
  private int maxContractsMultiplicity_;
  private int spaceContributedKb_;

  protected Timer timer_;
  private Provider<Timer> repairerTimerProvider_;
  private ContractsSelectionAlgorithm contractsSelectionAlgorithm_;

  /*
   * Enum describing owner of the group lock
   */
  private enum GroupLockState {
    NONE, BROKER, WRITE_MODULES
  }


  public class ValuationBrokerVisitor extends BrokerVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Initialized.");
      // setting contracts improvement, when a new peer is discovered
      MessageGenerator contractImprovementMessageGenerator = new MessageGenerator() {
        @Override
        public Message generate() {
          return new ImproveContractsMessage(jobId_);
        }
      };
      networkMonitor_.addContextChangeMessageGenerator(contractImprovementMessageGenerator);

      // setting periodical contracts improvement
      timer_.scheduleRepeated(new ImproveContractsMessage(jobId_),
          contractImprovementDelay_ * 1000, contractImprovementPeriod_ * 1000);

      executor_.scheduleAtFixedRate(new StatisticsCacheRefresherService(),
          STATS_REFRESHING_INITIAL_DELAY_SEC, STATS_REFRESHING_PERIOD_SEC, TimeUnit.SECONDS);
    }

    public void visit(BreakContractMessage message) {
      respondToBreakContractMessage(message.getContract(), message.getSourceAddress());
    }

    public void visit(AsyncBreakContractMessage message) {
      respondToBreakContractMessage(message.getContract(), message.getSenderAddress());
    }

    private void respondToBreakContractMessage(Contract contract, CommAddress sourceAddress) {
      contract.toLocalAndRemoteSwapped();
      if (contract.getPeer().equals(sourceAddress)) {
        logger_.debug("Broken: {}", contract.toString());
        removeContract(contract);
      } else {
        logger_.warn("Received request for breaking contract from an inappropiate peer");
      }
    }

    /**
     * @param message
     */
    public void visit(ImproveContractsMessage message) {
      logger_.debug("Improving contracts...");

      ContractsSet possibleContracts = new ContractsSet();

      Set<CommAddress> randomPeersSample = networkMonitor_.getRandomPeersSample();

      if (allContracts_.realSize() > spaceContributedKb_) {
        logger_.debug("Contributed size fully utilized.");
        return;
      }

      // todo(szm): temporarily using gossiped random peers sample
      // todo(szm): choosing peers to offer contracts should be somewhere different
      for (CommAddress commAddress : randomPeersSample) {
        if (getNumberOfContractsWith(commAddress) < maxContractsMultiplicity_ &&
            !addressLocks_.contains(commAddress) && !commAddress.equals(myAddress_)) {
          possibleContracts.add(new Contract(myAddress_, commAddress, defaultContractSizeKb_));
        }
      }

      if (possibleContracts.isEmpty()) {
        logger_.debug("No possible new contracts.");
      } else {
        logger_.debug("Possible contracts: {}", possibleContracts);
        if (unusedContracts_.size() < unusedContractsMaxNumber_) {
          final Contract toOffer =
              contractsSelectionAlgorithm_.chooseContractToOffer(possibleContracts, allContracts_);
          logger_.debug("Selected contract: {}", toOffer);
          // TODO(szm): timeout
          if (toOffer != null) {
            addressLocks_.add(toOffer.getPeer());
            startSessionAgreement(toOffer);
            possibleContracts = new ContractsSet(Sets.filter(possibleContracts,
                new Predicate<Contract>() {
                  @Override
                  public boolean apply(Contract contract) {
                    return !contract.getPeer().equals(toOffer.getPeer());
                  }
                }
            ));
          }
        }
      }

      possibleContracts.addAll(unusedContracts_);
      if (possibleContracts.isEmpty()) {
        logger_.debug("No contracts to improve groups");
      } else {
        logger_.debug("Possible contracts for groups: {}", possibleContracts);

        for (Entry<ReplicationGroup, List<Contract>> entry : groupsContracts_.entrySet()) {
          logger_.debug("Improving next replication group: {}", entry.getKey());
          logger_.debug("Current contracts in the group: {}", entry.getValue());
          ReplicationGroup group = entry.getKey();
          final List<CommAddress> replicators = Lists.newLinkedList(group.getReplicators());
          List<Contract> contracts = entry.getValue();

          if (groupLocks_.get(group).equals(GroupLockState.NONE)) {
            ImproveGroupResponse response = contractsSelectionAlgorithm_.
                improveGroupOfContracts(new ContractsSet(contracts),
                new ContractsSet(Sets.filter(possibleContracts, new Predicate<Contract>() {
                  @Override
                  public boolean apply(Contract contract) {
                    return !replicators.contains(contract.getPeer());
                  }
                })));

            // TODO(szm): timeout
            if (response.contractToAdd_ != null) {
              groupLocks_.put(group, GroupLockState.BROKER);
              addressLocks_.add(response.contractToAdd_.getPeer());

              Iterator<Contract> iterator = possibleContracts.iterator();
              while (iterator.hasNext()) {
                Contract currContract = iterator.next();
                if (currContract.getPeer().equals(response.contractToAdd_.getPeer())) {
                  iterator.remove();
                }
              }

              logger_.debug("Group: {}", group);

              GroupUpdateData updateData = new GroupUpdateData(response,
                  entry.getKey());
              if (unusedContracts_.contains(response.contractToAdd_)) {
                // We already have the required contract
                unusedContracts_.remove(response.contractToBreak_);
                CommAddress destPeer = response.contractToBreak_.getPeer();
                ReplicaRepairerModule replicaRepairer =
                    replicaRepairerFactory_.createRepairer(replicators, new ReplicatorData(
                    replicators.indexOf(destPeer), response.contractToAdd_),
                    Sets.newHashSet(objectsMap_.get(group)));
                replicaRepairer.runThroughDispatcher();
                Timer timer = repairerTimerProvider_.get();
                timer.schedule(
                    jobId_,
                    REPAIRER_MODULE_TIMEOUT,
                    new ReplicaRepairerData(response.contractToAdd_.getPeer(), replicaRepairer.
                    getJobId()));
                updateData.repairerModuleJobId_ = replicaRepairer.getJobId();
                updateData.timer_ = timer;
                logger_.debug("Contract to add: {}", response.contractToAdd_);
                logger_.debug("Contract to remove: {}", response.contractToBreak_);
                logger_.debug("Contracts: {}", contracts);
                logger_.debug("All contracts: {}", allContracts_);
              } else {
                startSessionAgreement(response.contractToAdd_);
              }
              groupUpdatesMap_.put(response.contractToAdd_.getPeer(), updateData);
            } else {
              logger_.debug("No contract selected to improve the group.");
            }
          } else {
            logger_.debug("Cannot improve group until previous improvement is finished.");
          }
        }
      }
    }

    public void visit(InitSessionEndMessage message) {
      logger_.debug("Process {}", message);
      CommAddress peerAddress = message.getPeerAddress();
      SecretKey sessionKey = message.getSessionKey();
      sessionKeys_.put(message.getSessionId(), sessionKey);
      try {
        EncryptedObject offer = encryptionAPI_.encryptWithSessionKey(message.getData(), sessionKey);
        networkQueue_.add(new ContractOfferMessage(getJobId(), peerAddress, offer, message
            .getSessionId()));
      } catch (CryptoException e) {
        clearInitSessionVariables(message.getSessionId());
        finishGroupUpdate(peerAddress);
      }
    }

    public void visit(InitSessionEndWithErrorMessage message) {
      logger_.debug("InitSessionEndWithErrorMessage {}", message.getErrorMessage());
      finishGroupUpdate(message.getPeerAddress());
    }

    public void visit(OfferReplyMessage message) {
      logger_.debug("Received offer reply message: {}", message);
      CommAddress peerAddress = message.getSourceAddress();
      Contract contract = null;
      SecretKey secretKey = sessionKeys_.remove(message.getSessionId());
      try {
        contract =
            (Contract) encryptionAPI_.decryptWithSessionKey(message.getEncryptedContract(),
                secretKey);
      } catch (CryptoException | NullPointerException e) {
        logger_.error(e.getMessage(), e);
        finishGroupUpdate(peerAddress);
        return;
      }

      contract.toLocalAndRemoteSwapped();
      if (message.getResult()) {
        logger_.debug("Contract concluded: {}", contract);
        GroupUpdateData data = groupUpdatesMap_.get(peerAddress);
        if (data == null) {
          addUnusedContract(contract);
          updateReplicationGroups();
        } else {
          CommAddress destPeer = data.response_.contractToBreak_.getPeer();
          ReplicaRepairerModule replicaRepairer =
              replicaRepairerFactory_.createRepairer(data.group_.getReplicators(),
              new ReplicatorData(data.group_.getReplicators().indexOf(destPeer),
              data.response_.contractToAdd_), Sets.newHashSet(objectsMap_.get(data.group_)));
          replicaRepairer.runThroughDispatcher();
          data.repairerModuleJobId_ = replicaRepairer.getJobId();
          Timer timer = repairerTimerProvider_.get();
          timer.schedule(
              jobId_, REPAIRER_MODULE_TIMEOUT,
              new ReplicaRepairerData(data.response_.contractToAdd_.getPeer(),
              replicaRepairer.getJobId()));
          data.timer_ = timer;
        }

        // todo(szm): przydzielanie przestrzeni adresowej do kontraktow
        // todo(szm): z czasem coraz rzadziej polepszam kontrakty

      } else {
        logger_.debug("Contract not concluded: {}", contract);
        finishGroupUpdate(peerAddress);
      }
      addressLocks_.remove(message.getSourceAddress());
      // todo(szm): timeouts
    }

    public void visit(ContractOfferMessage message) {
      if (!addressLocks_.contains(message.getSourceAddress())) {
        CommAddress peerAddress = message.getSourceAddress();
        String sessionId = message.getSessionId();
        contractOffer_.put(sessionId, message.getEncryptedContract());
        outQueue_.add(new GetSessionKeyMessage(peerAddress, getJobId(), sessionId));
      } else {
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            message.getEncryptedContract(), message.getSessionId(), false));
      }
    }

    public void visit(GetSessionKeyResponseMessage message) {
      CommAddress peerAddress = message.getPeerAddress();
      String sessionId = message.getSessionId();

      OfferResponse response;
      Contract offer = null;
      EncryptedObject encryptedOffer = null;
      try {
        offer =
            (Contract) encryptionAPI_.decryptWithSessionKey(contractOffer_.remove(sessionId),
                message.getSessionKey());

        offer.toLocalAndRemoteSwapped();
        encryptedOffer = encryptionAPI_.encryptWithSessionKey(offer, message.getSessionKey());
      } catch (CryptoException e) {
        logger_.error(e.getMessage(), e);
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress, encryptedOffer, sessionId,
            false));
        return;
      } finally {
        clearInitSessionVariables(sessionId);
      }

      if (allContracts_.realSize() > spaceContributedKb_ ||
          getNumberOfContractsWith(offer.getPeer()) >= maxContractsMultiplicity_ ||
          unusedContracts_.size() >= unusedContractsMaxNumber_) {
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress, encryptedOffer, sessionId,
            false));
        return;
      }

      response = contractsSelectionAlgorithm_.responseToOffer(offer, unusedContracts_);
      if (response.responseAnswer_) {
        logger_.debug("Concluding contract: {}", offer);
        addUnusedContract(offer);
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress, encryptedOffer, sessionId,
            true));
        for (Contract contract : response.contractsToBreak_) {
          sendBreakContractMessage(contract);
          removeContract(contract);
        }
        updateReplicationGroups();
      } else {
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress, encryptedOffer, sessionId,
            false));
      }
    }

    public void visit(RepairFailedMessage message) {
      GroupUpdateData data = groupUpdatesMap_.get(message.getPeer());
      if (data != null && data.repairerModuleJobId_.equals(message.getRepairerJobId())) {
        logger_.warn("Repairer module ended with an error.", message.getException());
        data.timer_.cancelTimer();
        failGroupRepair(data);
        finishGroupUpdate(message.getPeer());
      } else {
        logger_.info("Received a message with the result of an unknown repair");
      }
    }

    public void visit(RepairSuccessfulMessage message) {
      GroupUpdateData data = groupUpdatesMap_.get(message.getPeer());
      logger_.info("Group update data: " + data);
      logger_.info("Repairer job id: " + data.repairerModuleJobId_);
      logger_.info("Message id: " + message.getId());
      if (data != null && data.repairerModuleJobId_.equals(message.getRepairerJobId())) {
        data.timer_.cancelTimer();
        if (!allContracts_.contains(data.response_.contractToAdd_)) {
          addContract(data.response_.contractToAdd_);
        }
        List<Contract> contracts = groupsContracts_.get(data.group_);
        contracts.set(contracts.indexOf(data.response_.contractToBreak_),
            data.response_.contractToAdd_);
        data.group_.swapReplicators(data.response_.contractToBreak_.getPeer(),
            data.response_.contractToAdd_.getPeer());

        try {
          putReplicationGroups(replicationGroupUpdateTimeout_);
        } catch (NebuloException e) {
          logger_.warn("Unable to put replication groups into DHT", e);
          //revert changes
          failGroupRepair(data);
          contracts.set(contracts.indexOf(data.response_.contractToAdd_),
              data.response_.contractToBreak_);
          data.group_.swapReplicators(data.response_.contractToAdd_.getPeer(),
              data.response_.contractToBreak_.getPeer());
          finishGroupUpdate(message.getPeer());
          return;
        }

        sendBreakContractMessage(data.response_.contractToBreak_);
        removeContract(data.response_.contractToBreak_);
        finishGroupUpdate(message.getPeer());
      } else {
        logger_.debug("Received a message with the result of an unknown repair");
      }
    }

    public void visit(TimeoutMessage message) {
      if (message.getMessageContent() instanceof ReplicaRepairerData) {
        ReplicaRepairerData repairerData = (ReplicaRepairerData) message.getMessageContent();
        GroupUpdateData updateData = groupUpdatesMap_.get(repairerData.peer_);
        if (repairerData.jobId_.equals(updateData.repairerModuleJobId_)) {
          logger_.warn("Repairer module with jobId: {} reached the timeout. " +
              "Repair will be aborted!", repairerData.jobId_);
          failGroupRepair(updateData);
          finishGroupUpdate(repairerData.peer_);
        } else {
          logger_.debug("Received a timeout message about an unknown repair");
        }
      } else {
        logger_.debug("Received an unexpected timeout message");
      }
    }

    public void visit(CheckContractMessage message) {
      logger_.debug("CheckContractMessage Peer {}", message.getContractPeer());
      outQueue_.add(message.getResponse(getNumberOfContractsWith(message.getContractPeer()) > 0 ||
          message.getContractPeer().equals(myAddress_)));
    }

    public void visit(WritePermissionRequestMessage message) {
      ReplicationGroup group = findGroupByObjectId(message.getObjectId());

      if (group == null) {
        logger_.warn("No group for objectId: " + message.getObjectId() + " found.");
        outQueue_.add(new WritePermissionRejectionMessage(message.getWriteModuleJobId()));
      } else if (!groupLocks_.get(group).equals(GroupLockState.BROKER)) {
        outQueue_.add(new WritePermissionResponseMessage(message.getWriteModuleJobId()));
        groupLocks_.put(group, GroupLockState.WRITE_MODULES);
      } else {
        waitingWriteModules_.get(group).add(message.getWriteModuleJobId());
      }
    }

    public void visit(WriteFinishedMessage message) {
      ReplicationGroup group = findGroupByObjectId(message.getObjectId());

      if (group == null) {
        logger_.warn("No group for objectId: " + message.getObjectId() + " found.");
      } else {
        runningWriteModules_.get(group).remove(message.getWriteModuleJobId());
        if (runningWriteModules_.get(group).isEmpty()) {
          groupLocks_.put(group, GroupLockState.NONE);
        }
      }
    }

    public void visit(ErrorCommMessage message) {
      logger_.debug("Received: {}", message);
      if (message.getMessage() instanceof ContractOfferMessage) {
        ContractOfferMessage offerMessage = (ContractOfferMessage) message.getMessage();
        clearInitSessionVariables(offerMessage.getSessionId());
        finishGroupUpdate(offerMessage.getDestinationAddress());
      }
    }

    public void visit(EndModuleMessage message) {
      logger_.debug("Ending the broker module");
      timer_.cancelTimer();
      executor_.shutdownNow();
      try {
        executor_.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger_.warn("Error while waiting for executor termination.", e);
      }
      endJobModule();
    }

    private void failGroupRepair(GroupUpdateData data) {
      if (unusedContracts_.size() < unusedContractsMaxNumber_) {
        addUnusedContract(data.response_.contractToAdd_);
      } else {
        sendBreakContractMessage(data.response_.contractToAdd_);
      }
    }

    private ReplicationGroup findGroupByObjectId(ObjectId objectId) {
      ReplicationGroup objectGroup = null;
      for (ReplicationGroup group : groupsContracts_.keySet()) {
        if (group.fitsIntoGroup(objectId)) {
          objectGroup = group;
          break;
        }
      }
      return objectGroup;
    }

  }

  @Override
  public void initialize(Metadata metadata) {
    super.initialize(metadata);

    for (ReplicationGroup group : groupsContracts_.keySet()) {
      groupLocks_.put(group, GroupLockState.NONE);
      waitingWriteModules_.put(group, new HashSet<String>());
      runningWriteModules_.put(group, new HashSet<String>());
    }
  }

  @Override
  public void addReplicationGroup(ReplicationGroup group, List<Contract> contracts) {
    super.addReplicationGroup(group, contracts);
    groupLocks_.put(group, GroupLockState.NONE);
    waitingWriteModules_.put(group, new HashSet<String>());
    runningWriteModules_.put(group, new HashSet<String>());
  }

  private void startSessionAgreement(Contract offer) {
    logger_.debug("Starting session agreement with {}", offer.getPeer());
    outQueue_.add(new LocalInitSessionMessage(offer.getPeer(), getJobId(), offer));
  }

  private void clearInitSessionVariables(String sessionId) {
    sessionKeys_.remove(sessionId);
    contractOffer_.remove(sessionId);
  }

  private void finishGroupUpdate(CommAddress peerAddress) {
    GroupUpdateData data = groupUpdatesMap_.remove(peerAddress);
    if (data != null) {
      if (waitingWriteModules_.get(data.group_).isEmpty()) {
        groupLocks_.put(data.group_, GroupLockState.NONE);
      } else {
        runningWriteModules_.get(data.group_).addAll(waitingWriteModules_.get(data.group_));
        waitingWriteModules_.get(data.group_).clear();
        for (String writeModuleJobId : runningWriteModules_.get(data.group_)) {
          outQueue_.add(new WritePermissionResponseMessage(writeModuleJobId));
        }
        groupLocks_.put(data.group_, GroupLockState.WRITE_MODULES);
      }
    }
    addressLocks_.remove(peerAddress);
  }

  private void updateReplicationGroups() {
    try {
      updateReplicationGroups(replicationGroupUpdateTimeout_);
    } catch (NebuloException e) {
      logger_.warn("Unsuccessful DHT update.", e);
    }
  }

  private void sendBreakContractMessage(Contract contract) {
    networkQueue_.add(new BreakContractMessage(null, contract.getPeer(), contract));
  }

  private static class GroupUpdateData {

    public ImproveGroupResponse response_;
    public ReplicationGroup group_;
    public String repairerModuleJobId_;
    public Timer timer_;

    public GroupUpdateData(ImproveGroupResponse response, ReplicationGroup group,
        String repairerModuleJobId, Timer timer) {
      response_ = response;
      group_ = group;
      repairerModuleJobId_ = repairerModuleJobId;
      timer_ = timer;
    }

    public GroupUpdateData(ImproveGroupResponse response, ReplicationGroup group) {
      this(response, group, null, null);
    }
  }

  private static class ReplicaRepairerData {

    public CommAddress peer_;
    public String jobId_;

    public ReplicaRepairerData(CommAddress peer, String jobId) {
      peer_ = peer;
      jobId_ = jobId;
    }
  }

  private class StatisticsCacheRefresherService implements Runnable {

    private static final int GET_STATS_MODULE_TIMEOUT_SEC = 5;

    @Override
    public void run() {
      logger_.debug("Starting refresher service");
      logger_.debug("Known peers number: " + networkMonitor_.getKnownPeers().size());
      if (networkMonitor_.getKnownPeers().size() > 0) {
        List<CommAddress> knownPeers = networkMonitor_.getKnownPeers();
        logger_.debug("Known peers: " + knownPeers);
        Collections.shuffle(knownPeers);
        ReturningJobModule<Map<CommAddress, StatisticsList>> getStatsModule =
            new GetPeersStatisticsModule(Sets.newHashSet(knownPeers.subList(0,
            Math.min(STATS_REFRESHING_PEERS_NUMBER, knownPeers.size()))), outQueue_);
        getStatsModule.runThroughDispatcher();
        logger_.debug("Run through dispatcher");

        Map<CommAddress, StatisticsList> result;
        try {
          result = getStatsModule.getResult(GET_STATS_MODULE_TIMEOUT_SEC);
        } catch (NebuloException e) {
          logger_.warn("Failed to refresh statistics cache", e);
          return;
        }
        logger_.debug("Got result");

        networkMonitor_.putAllStatistics(result);
        logger_.debug("Put statistics");
        logger_.debug("Currently statistics for " + networkMonitor_.getStatisticsMap().size() +
            " peers (" + networkMonitor_.getStatisticsMap().keySet() + ") are available.");
      }
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
