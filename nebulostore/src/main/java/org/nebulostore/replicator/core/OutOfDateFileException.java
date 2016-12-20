package org.nebulostore.replicator.core;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Thrown when trying to update file that is out of date.
 * @author szymonmatejczyk
 *
 */
public class OutOfDateFileException extends NebuloException {
  private static final long serialVersionUID = 2826258091291600719L;

}
