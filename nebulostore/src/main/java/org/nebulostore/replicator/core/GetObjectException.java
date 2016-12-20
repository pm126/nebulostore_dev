package org.nebulostore.replicator.core;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * @author szymonmatejczyk
 */
public class GetObjectException extends NebuloException {
  private static final long serialVersionUID = -5577241173591330754L;

  final String message_;

  public String getMessage() {
    return message_;
  }

  public GetObjectException(String message) {
    super();
    message_ = message;
  }
}
