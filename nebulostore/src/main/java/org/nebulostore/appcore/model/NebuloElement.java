package org.nebulostore.appcore.model;

import java.io.Serializable;
import java.math.BigInteger;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Element of a list (currently raw data or NebuloAddress).
 * @author bolek
 */
public class NebuloElement implements Serializable {
  private static final long serialVersionUID = -867504311256037343L;

  // Unique id for list element used for consistency purposes.
  protected BigInteger elementId_;

  // Only one of these is used.
  protected NebuloAddress address_;
  protected EncryptedObject innerObject_;

  /**
   * Creates a new link to existing NebuloObject denoted by address.
   * @param address  address of NebuloObject that this NebuloElement will point to
   */
  public NebuloElement(NebuloAddress address) {
    address_ = address;
    elementId_ = CryptoUtils.getRandomId();
  }

  /**
   * Creates a new link to existing NebuloObject.
   * @param object  object that this NebuloElement refers to
   */
  public NebuloElement(NebuloObject object) {
    address_ = object.getAddress();
    elementId_ = CryptoUtils.getRandomId();
  }

  /**
   * Creates a new element containing some data of unknown structure.
   * @param object  data to hold
   */
  public NebuloElement(EncryptedObject object) {
    innerObject_ = object;
    elementId_ = CryptoUtils.getRandomId();
  }

  public boolean isLink() {
    return address_ != null;
  }

  public NebuloAddress getAddress() {
    return address_;
  }

  public EncryptedObject getData() {
    return innerObject_;
  }

  public ObjectId getObjectId() {
    return address_.getObjectId();
  }

}
