package org.nebulostore.conductor.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message used to finish tests immediatelly. Send by TestingServer to clients.
 * @author szymonmatejczyk
 *
 */
public class FinishMessage extends CommMessage {
  private static final long serialVersionUID = -1391650791568026886L;

  public FinishMessage(CommAddress sourceAddress, CommAddress destAddress) {
    super(sourceAddress, destAddress);
  }

  public FinishMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }

}
