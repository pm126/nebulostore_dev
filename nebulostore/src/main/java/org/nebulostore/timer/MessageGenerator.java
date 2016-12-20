package org.nebulostore.timer;

import org.nebulostore.appcore.messaging.Message;

/**
 * Generates a message.
 * @author szymonmatejczyk
 */
public interface MessageGenerator {
  Message generate();
}
