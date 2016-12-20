package org.nebulostore.appcore.messaging;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * @author bolek
 * Exception thrown from visitor after receiving message of unsupported type.
 */
public class UnsupportedMessageException extends NebuloException {
  private static final long serialVersionUID = 8031917232378781743L;

  public UnsupportedMessageException(String message) {
    super(message);
  }
}
