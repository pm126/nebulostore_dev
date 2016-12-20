package org.nebulostore.broker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import com.google.common.base.Predicate;

/**
 * Set of contracts.
 */
public class ContractsSet extends HashSet<Contract> implements Cloneable {
  private static final long serialVersionUID = 112351L;

  public ContractsSet() {
    super();
  }

  /**
   * Singleton constructor.
   */
  public ContractsSet(Contract contract) {
    super();
    add(contract);
  }

  public ContractsSet(Collection<Contract> contracts) {
    super(contracts);
  }

  public long realSize() {
    long size = 0;
    Iterator<Contract> it = iterator();
    while (it.hasNext()) {
      size += it.next().getSize();
    }
    return size;
  }

  @Override
  public ContractsSet clone() {
    return (ContractsSet) super.clone();
  }

  public static Predicate<Contract> containsPredicate(final ContractsSet set) {
    return new Predicate<Contract>() {
      @Override
      public boolean apply(Contract contract) {
        return set.contains(contract);
      }
    };
  }
}
