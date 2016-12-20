package org.nebulostore.dfuntest.coding.messages;

import java.util.List;

import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.coding.CodingTestHelperMessageForwarder;

public class TestNebuloObjectsMessage extends CommMessage {

  private static final long serialVersionUID = -653930550235794619L;

  private final List<NebuloFile> nebuloObjects_;

  public TestNebuloObjectsMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, List<NebuloFile> nebuloObjects) {
    super(jobId, sourceAddress, destAddress);
    nebuloObjects_ = nebuloObjects;
  }

  public List<NebuloFile> getNebuloObjects() {
    return nebuloObjects_;
  }

  @Override
  public JobModule getHandler() {
    return new CodingTestHelperMessageForwarder(this);
  }

}
