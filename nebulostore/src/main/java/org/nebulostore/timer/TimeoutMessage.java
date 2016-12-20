package org.nebulostore.timer;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message issued by Timer.
 * @author szymonmatejczyk
 */
public class TimeoutMessage extends Message {
  private static final long serialVersionUID = -8674965519068356105L;
  private final Object messageContent_;

  public TimeoutMessage(String jobID, Object messageContent) {
    super(jobID);
    messageContent_ = messageContent;
  }

  public Object getMessageContent() {
    return messageContent_;
  }

}
