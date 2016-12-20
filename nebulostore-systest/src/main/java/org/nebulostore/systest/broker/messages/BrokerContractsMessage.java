package org.nebulostore.systest.broker.messages;

import java.util.Set;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.broker.Contract;

/**
 * Message send by Broker with its contracts.
 *
 * @author szymonmatejczyk
 */
public class BrokerContractsMessage extends Message {
  private static final long serialVersionUID = -6196721928597455534L;

  private final Set<Contract> allContracts_;

  public BrokerContractsMessage(String jobId, Set<Contract> allContracts) {
    super(jobId);
    allContracts_ = allContracts;
  }

  public Set<Contract> getAllContracts() {
    return allContracts_;
  }
}
