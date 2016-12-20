package org.nebulostore.appcore.addressing;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Thrown when trying to add overlapping intervals to ContractList.
 */
public class IntervalCollisionException extends NebuloException {
  private static final long serialVersionUID = -7966643647636406941L;
}
