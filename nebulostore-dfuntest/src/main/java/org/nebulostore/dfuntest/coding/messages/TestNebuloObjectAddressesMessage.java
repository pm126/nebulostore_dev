package org.nebulostore.dfuntest.coding.messages;

import java.util.List;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.coding.CodingTestHelperMessageForwarder;

public class TestNebuloObjectAddressesMessage extends CommMessage {

  private static final long serialVersionUID = -653930550235794619L;

  private final List<NebuloAddress> nebuloObjectAddresses_;

  public TestNebuloObjectAddressesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, List<NebuloAddress> nebuloObjectAddresses) {
    super(jobId, sourceAddress, destAddress);
    nebuloObjectAddresses_ = nebuloObjectAddresses;
  }

  public List<NebuloAddress> getNebuloObjectAddresses() {
    return nebuloObjectAddresses_;
  }

  @Override
  public JobModule getHandler() {
    return new CodingTestHelperMessageForwarder(this);
  }

}
