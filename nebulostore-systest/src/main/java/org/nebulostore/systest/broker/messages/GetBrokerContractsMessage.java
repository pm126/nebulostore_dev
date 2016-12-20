package org.nebulostore.systest.broker.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message send to broker to get its context.
 *
 * @author szymonmatejczyk
 *
 */
public class GetBrokerContractsMessage extends Message {
  private static final long serialVersionUID = 5150296318185156609L;

  public GetBrokerContractsMessage(String jobId) {
    super(jobId);
  }
}
