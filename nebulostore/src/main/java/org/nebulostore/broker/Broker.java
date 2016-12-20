package org.nebulostore.broker;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.IntervalCollisionException;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.messages.RegisterObjectMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.networkmonitor.NetworkMonitor;

/**
 * Broker is always a singleton job. See BrokerMessageForwarder.
 *
 * @author Bolek Kulbabinski
 */
public abstract class Broker extends JobModule {

  protected static final String CONFIGURATION_PREFIX = "broker.";
  private static final String REPLICATION_GROUP_SIZE_NAME = CONFIGURATION_PREFIX +
      "replication-group-size";
  protected static final String UNUSED_CONTRACTS_MAX_NUMBER_NAME = CONFIGURATION_PREFIX +
      "unused-contracts-max-number";

  private static Logger logger_ = Logger.getLogger(Broker.class);

  private static final int METADATA_UPDATE_TIMEOUT_SEC = 10;

  protected CommAddress myAddress_;
  protected NetworkMonitor networkMonitor_;
  private AppKey appKey_;
  protected EncryptionAPI encryptionAPI_;
  protected String privateKeyPeerId_;
  private int replicationGroupSize_;
  protected int unusedContractsMaxNumber_;

  protected final ContractsSet unusedContracts_ = new ContractsSet();
  protected final ContractsSet allContracts_ = new ContractsSet();
  protected final Map<CommAddress, List<Contract>> contractMap_ =
      new ConcurrentHashMap<CommAddress, List<Contract>>();
  protected final Map<ReplicationGroup, List<Contract>> groupsContracts_ = new HashMap<>();
  protected final Map<ReplicationGroup, Set<ObjectId>> objectsMap_ = new HashMap<>();

  /**
   * Available space for contract.
   */
  private final Map<Contract, Integer> freeSpaceMap_ = new HashMap<Contract, Integer>();

  /**
   * Contract offers sent to peers, waiting for response.
   */
  protected final Map<CommAddress, Contract> contractOffers_ = new ConcurrentHashMap<>();

  protected MessageVisitor visitor_;

  @Inject
  private void setDependencies(CommAddress myAddress, NetworkMonitor networkMonitor,
      AppKey appKey, EncryptionAPI encryptionAPI,
      @Named("PrivateKeyPeerId") String privateKeyPeerId,
      @Named(REPLICATION_GROUP_SIZE_NAME) int replicationGroupSize,
      @Named(UNUSED_CONTRACTS_MAX_NUMBER_NAME) int unusedContractsMaxNumber) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
    appKey_ = appKey;
    privateKeyPeerId_ = privateKeyPeerId;
    encryptionAPI_ = encryptionAPI;
    replicationGroupSize_ = replicationGroupSize;
    unusedContractsMaxNumber_ = unusedContractsMaxNumber;
  }

  //Initialize broker using the data from DHT
  public void initialize(Metadata metadata) {
    logger_.debug("Group contracts downloaded from DHT in metadata object: " +
        metadata.getGroupsContracts());
    logger_.debug("Object Ids downloaded from DHT in metadata object: " +
        metadata.getObjectsMap());
    logger_.debug("Contract list downloaded from DHT in metadata object: " +
        metadata.getContractList());
    logger_.debug("All contracts downloaded from DHT in metadata object: " +
        metadata.getAllContracts());
    logger_.debug("Metadata object downloaded from DHT: " + metadata);

    if (metadata.getAllContracts() != null) {
      allContracts_.addAll(metadata.getAllContracts());
    }
    if (metadata.getUnusedContracts() != null) {
      unusedContracts_.addAll(metadata.getUnusedContracts());
    }
    if (metadata.getGroupsContracts() != null) {
      groupsContracts_.putAll(metadata.getGroupsContracts());
    }
    if (metadata.getObjectsMap() != null) {
      objectsMap_.putAll(metadata.getObjectsMap());
    }

    for (Contract contract : allContracts_) {
      CommAddress peer = contract.getPeer();
      if (!contractMap_.containsKey(peer)) {
        contractMap_.put(peer, new LinkedList<Contract>());
      }
      contractMap_.get(peer).add(contract);
    }
  }

  public Map<CommAddress, List<Contract>> getContractList() {
    return Collections.unmodifiableMap(contractMap_);
  }

  public void updateReplicationGroups(int timeoutSec) throws NebuloException {
    // todo(szm): one group for now
    ContractList contractList = new ContractList();
    ReplicationGroup group;
    if (groupsContracts_.isEmpty()) {
      List<CommAddress> commAddresses = new LinkedList<>();
      List<Contract> selectedContracts = new LinkedList<>();
      Iterator<Contract> iterator = unusedContracts_.iterator();
      commAddresses.add(myAddress_);
      while (iterator.hasNext() && commAddresses.size() < replicationGroupSize_) {
        Contract contract = iterator.next();
        if (!commAddresses.contains(contract.getPeer())) {
          commAddresses.add(contract.getPeer());
          selectedContracts.add(contract);
        }
      }

      if (commAddresses.size() == replicationGroupSize_) {
        CommAddress[] addresses = new CommAddress[commAddresses.size()];
        group =
            new ReplicationGroup(commAddresses.toArray(addresses), BigInteger.ZERO, new BigInteger(
                "1000000"));

        try {
          contractList.addGroup(group);
        } catch (IntervalCollisionException e) {
          throw new NebuloException("Error while creating replication group", e);
        }

        addReplicationGroup(group, selectedContracts);

        logger_.debug("Current group contracts map: " + groupsContracts_);
        Metadata metadata = addBrokerMetadata(new Metadata(appKey_, contractList));
        logger_.debug("Group contracts in metadata: " + metadata.getGroupsContracts());
        logger_.debug("Metadata object prepared to put into DHT: " + metadata);

        PutKeyModule putKeyModule =
            new PutKeyModule(outQueue_, new KeyDHT(appKey_.getKey()), new ValueDHT(
            metadata));
        try {
          putKeyModule.getResult(timeoutSec);
        } catch (NebuloException e) {
          revertGroupAddition(group);
          throw new NebuloException("Failed to put groups in DHT.", e);
        }
      }
    }
  }

  protected void addUnusedContract(Contract contract) {
    unusedContracts_.add(contract);
    addContract(contract);
  }

  protected void addContract(Contract contract) {
    logger_.debug("Adding contract with: " + contract.getPeer().toString());
    allContracts_.add(contract);
    if (contractMap_.containsKey(contract.getPeer())) {
      contractMap_.get(contract.getPeer()).add(contract);
    } else {
      List<Contract> contractList = new ArrayList<Contract>();
      contractList.add(contract);
      contractMap_.put(contract.getPeer(), contractList);
    }
    updateMetadata();
  }

  protected void removeContract(Contract contract) {
    logger_.debug("Removing contract with: " + contract.getPeer().toString());
    if (allContracts_.contains(contract)) {
      logger_.debug("Contract " + contract + " is present in all contracts");
      if (unusedContracts_.contains(contract)) {
        logger_.debug("Contract " + contract + " is present in unused contracts");
        allContracts_.remove(contract);
        unusedContracts_.remove(contract);
        contractMap_.get(contract.getPeer()).remove(contract);
      } else {
        // Cannot remove this contract, only marking it as unavailable
        logger_.debug("Marking the contract: " + contract + " as unavailable.");
        List<Contract> peerContracts = contractMap_.get(contract.getPeer());
        peerContracts.get(peerContracts.indexOf(contract)).setAvailable(false);
      }
      updateMetadata();
    } else {
      logger_.warn("Removing non existing contract with: " + contract.getPeer().toString());
    }
  }

  protected CommAddress[] getReplicas() {
    CommAddress[] addresses = new CommAddress[allContracts_.size()];
    Iterator<Contract> iter = allContracts_.iterator();
    int i = 0;
    while (iter.hasNext()) {
      addresses[i] = iter.next().getPeer();
      i++;
    }
    return addresses;
  }

  protected void putReplicationGroups(int timeoutSec) throws NebuloException {
    ContractList contractList = new ContractList();
    try {
      for (ReplicationGroup group : groupsContracts_.keySet()) {
        contractList.addGroup(group);
      }
    } catch (IntervalCollisionException e) {
      throw new NebuloException("Error while putting replication groups", e);
    }

    PutKeyModule putKeyModule = new PutKeyModule(outQueue_, new KeyDHT(appKey_.getKey()),
        new ValueDHT(new Metadata(appKey_, contractList)));
    putKeyModule.getResult(timeoutSec);
  }

  protected void addReplicationGroup(ReplicationGroup group, List<Contract> contracts) {
    unusedContracts_.removeAll(contracts);
    groupsContracts_.put(group, Lists.newArrayList(contracts));
    objectsMap_.put(group, new HashSet<ObjectId>());
  }

  protected int getNumberOfContractsWith(CommAddress id) {
    List<Contract> contracts = contractMap_.get(id);
    if (contracts == null) {
      return 0;
    } else {
      return contracts.size();
    }
  }

  private void updateMetadata() {
    Metadata metadata = new Metadata(appKey_);
    addBrokerMetadata(metadata);
    PutKeyModule putKeyModule = new PutKeyModule(outQueue_, new KeyDHT(appKey_.getKey()),
        new ValueDHT(metadata));
    try {
      putKeyModule.getResult(METADATA_UPDATE_TIMEOUT_SEC);
    } catch (NebuloException e) {
      logger_.warn("Could not update metadata in DHT");
    }
  }

  private Metadata addBrokerMetadata(Metadata metadata) {
    metadata.setUnusedContracts(unusedContracts_);
    metadata.setAllContracts(allContracts_);
    metadata.setGroupsContracts(groupsContracts_);
    metadata.setObjectsMap(objectsMap_);
    return metadata;
  }

  private void revertGroupAddition(ReplicationGroup group) {
    List<Contract> groupContracts = groupsContracts_.remove(group);
    unusedContracts_.addAll(groupContracts);
    objectsMap_.remove(group);
  }

  protected class BrokerVisitor extends MessageVisitor {

    public void visit(RegisterObjectMessage message) {
      for (ReplicationGroup group : groupsContracts_.keySet()) {
        if (group.fitsIntoGroup(message.getObjectId())) {
          objectsMap_.get(group).add(message.getObjectId());
          updateMetadata();
          break;
        }
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
