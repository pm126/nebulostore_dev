package org.nebulostore.appcore.addressing;

import java.io.Serializable;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Application Key, identifies the user.
 *
 * The AppKey is represented by a 128 bit positive BigInteger in 2 complement
 * notation. Any other value causes an exception.
 * (immutable)
 */
public final class AppKey implements Serializable {
  private static final long serialVersionUID = -5977296486784377545L;
  private static final int MAX_BIT_SIZE = 128;
  private final BigInteger key_;

  public AppKey(BigInteger key) {
    checkNotNull(key);
    if (key.compareTo(BigInteger.ZERO) == -1 ||
        key.bitLength() > MAX_BIT_SIZE) {
      throw new IllegalArgumentException("key has to be positive and smaller than 1<<128");
    }
    key_ = key;
  }

  public AppKey(String key) {
    this(new BigInteger(key));
  }

  public BigInteger getKey() {
    return key_;
  }

  @Override
  public int hashCode() {
    return key_.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o != null && (o instanceof AppKey) && (((AppKey) o).key_.equals(key_))) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "AppKey[" + key_.toString() + "]";
  }
}
