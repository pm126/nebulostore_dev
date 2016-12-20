package org.nebulostore.appcore.addressing;

import java.io.Serializable;


/**
 * (AppKey, ObjectId) pair uniquely identifying object.
 * (immutable)
 */
public final class NebuloAddress implements Serializable {
  private static final long serialVersionUID = 673007553164628096L;
  private final AppKey appKey_;
  private final ObjectId objectId_;

  public NebuloAddress(AppKey appKey, ObjectId objectId) {
    appKey_ = appKey;
    objectId_ = objectId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (getClass() != obj.getClass()) {
      return false;
    }
    NebuloAddress other = (NebuloAddress) obj;
    if (appKey_ == null) {
      if (other.appKey_ != null) {
        return false;
      }
    } else if (!appKey_.equals(other.appKey_)) {
      return false;
    }
    if (objectId_ == null) {
      if (other.objectId_ != null) {
        return false;
      }
    } else if (!objectId_.equals(other.objectId_)) {
      return false;
    }
    return true;
  }

  public ObjectId getObjectId() {
    return objectId_;
  }

  public AppKey getAppKey() {
    return appKey_;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((appKey_ == null) ? 0 : appKey_.hashCode());
    result = prime * result + ((objectId_ == null) ? 0 : objectId_.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "NebuloAddress{" + appKey_ + "," + objectId_ + "}";
  }
}
