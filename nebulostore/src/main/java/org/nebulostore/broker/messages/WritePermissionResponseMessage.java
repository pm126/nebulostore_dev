package org.nebulostore.broker.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * @author Piotr Malicki
 */
public class WritePermissionResponseMessage extends Message {

  private static final long serialVersionUID = -130085506221101529L;

  public WritePermissionResponseMessage(String jobId) {
    super(jobId);
  }
}
