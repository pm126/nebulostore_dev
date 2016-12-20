package org.nebulostore.broker.messages;

import org.nebulostore.appcore.messaging.Message;

public class WritePermissionRejectionMessage extends Message {

  private static final long serialVersionUID = -5897979456844018286L;

  public WritePermissionRejectionMessage(String jobId) {
    super(jobId);
  }

}
