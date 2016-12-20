package org.nebulostore.replicator.core;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * @author szymonmatejczyk
 */
public class DeleteObjectException extends NebuloException {
  private static final long serialVersionUID = 1946442716458813208L;

  public DeleteObjectException(String message) {
    super(message);
  }

  public DeleteObjectException(String message, Exception cause) {
    super(message, cause);
  }
}
