package org.nebulostore.appcore.model;

import java.io.Serializable;
import java.util.Arrays;
/**
 * Wrapper for encrypted data. EncryptedObjects are stored in Replicator.
 * @author bolek
 */
public class EncryptedObject implements Serializable {
  private static final long serialVersionUID = -4442741001160904578L;

  private final byte[] encryptedData_;

  public EncryptedObject(byte[] encryptedData) {
    encryptedData_ = Arrays.copyOf(encryptedData, encryptedData.length);
  }

  public byte[] getEncryptedData() {
    return encryptedData_;
  }

  public int size() {
    return encryptedData_.length;
  }

  @Override
  public String toString() {
    return Arrays.toString(encryptedData_);
  }
}
