package org.nebulostore.appcore;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.broker.Contract;
import org.nebulostore.broker.ContractsSet;
import org.nebulostore.dht.core.Mergeable;

/**
 * Metadata object is stored in main DHT. It contains data necessary for system, that cannot be
 * stored in as other files, because they are used for replica management. It also stores user's
 * ContractList.
 *
 * @author szymonmatejczyk
 * @author bolek
 */
public class Metadata implements Mergeable, Serializable {
  private static final long serialVersionUID = 8900375455728664721L;

  /* Id of user, that this metadata applies to. */
  private final AppKey owner_;

  private ContractList contractList_;
  private ContractsSet unusedContracts_;
  private ContractsSet allContracts_;
  private Map<ReplicationGroup, List<Contract>> groupsContracts_;
  private Map<ReplicationGroup, Set<ObjectId>> objectsMap_;

  public Metadata(AppKey owner) {
    owner_ = owner;
  }

  public Metadata(AppKey owner, ContractList contractList) {
    owner_ = owner;
    contractList_ = contractList;
  }

  public AppKey getOwner() {
    return owner_;
  }

  public ContractList getContractList() {
    return contractList_;
  }

  public void setContractList(ContractList contractList) {
    contractList_ = contractList;
  }

  public ContractsSet getUnusedContracts() {
    return unusedContracts_;
  }

  public void setUnusedContracts(ContractsSet unusedContracts) {
    unusedContracts_ = unusedContracts;
  }

  public ContractsSet getAllContracts() {
    return allContracts_;
  }

  public void setAllContracts(ContractsSet allContracts) {
    allContracts_ = allContracts;
  }

  public Map<ReplicationGroup, List<Contract>> getGroupsContracts() {
    return groupsContracts_;
  }

  public void setGroupsContracts(Map<ReplicationGroup, List<Contract>> groupContracts) {
    groupsContracts_ = groupContracts;
  }

  public Map<ReplicationGroup, Set<ObjectId>> getObjectsMap() {
    return objectsMap_;
  }

  public void setObjectsMap(Map<ReplicationGroup, Set<ObjectId>> objectsMap) {
    objectsMap_ = objectsMap;
  }

  @Override
  public String toString() {
    return "Metadata [ owner : ( " + owner_ + " ), contractList: ( " + contractList_ + " ) ]";
  }

  @Override
  public Mergeable merge(Mergeable other) {
    if (other instanceof Metadata) {
      Metadata metadata = (Metadata) other;
      if (contractList_ == null) {
        contractList_ = metadata.contractList_;
      }
      if (unusedContracts_ == null) {
        unusedContracts_ = metadata.unusedContracts_;
      }
      if (allContracts_ == null) {
        allContracts_ = metadata.allContracts_;
      }
      if (groupsContracts_ == null) {
        groupsContracts_ = metadata.groupsContracts_;
      }
      if (objectsMap_ == null) {
        objectsMap_ = metadata.objectsMap_;
      }
    }
    return this;
  }

}
