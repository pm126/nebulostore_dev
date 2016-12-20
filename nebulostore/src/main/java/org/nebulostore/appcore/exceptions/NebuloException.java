package org.nebulostore.appcore.exceptions;

/**
 * Base class for NebuloStore exceptions.
 * @author Bolek Kulbabinski
 */
public class NebuloException extends Exception {
  private static final long serialVersionUID = 2550482529210695435L;

  public NebuloException() {
  }

  public NebuloException(String message) {
    super(message);
  }

  public NebuloException(Throwable cause) {
    super(cause);
  }

  public NebuloException(String message, Throwable cause) {
    super(message, cause);
  }
}
