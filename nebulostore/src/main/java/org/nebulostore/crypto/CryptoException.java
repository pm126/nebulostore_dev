package org.nebulostore.crypto;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Exceptions thrown from CryptoUtils class.
 */
public class CryptoException extends NebuloException {
  private static final long serialVersionUID = -8072269531093584325L;

  public CryptoException(String message) {
    super(message);
  }

  public CryptoException(String message, Exception cause) {
    super(message, cause);
  }
}
