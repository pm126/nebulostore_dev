package org.nebulostore.appcore.addressing;

import java.io.Serializable;
import java.math.BigInteger;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Unique integer ID that identifies object stored in NebuloStore.
 * (immutable)
 */
@XmlRootElement
public final class ObjectId implements Serializable {
  private static final long serialVersionUID = 1687973599624381804L;
  private final BigInteger key_;

  public ObjectId(BigInteger key) {
    key_ = key;
  }

  public BigInteger getKey() {
    return key_;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key_ == null) ? 0 : key_.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ObjectId other = (ObjectId) obj;
    if (key_ == null) {
      if (other.key_ != null) {
        return false;
      }
    } else if (!key_.equals(other.key_)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "ObjectId[" + key_.toString() + "]";
  }
}
