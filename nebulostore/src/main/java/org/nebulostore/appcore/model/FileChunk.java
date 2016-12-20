package org.nebulostore.appcore.model;

import java.util.Arrays;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Represents file chunk.
 */
public class FileChunk extends NebuloObject {
  private static final long serialVersionUID = -8992804393601229112L;

  // Plain, unencrypted data.
  protected byte[] data_;

  public FileChunk(NebuloAddress address, int len) {
    super(address);
    data_ = new byte[len];
  }

  public byte[] getData() {
    return data_;
  }

  public void setData(byte[] data) {
    data_ = Arrays.copyOf(data, data.length);
  }

  @Override
  protected void runSync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete() throws NebuloException {
    throw new UnsupportedOperationException();
  }
}
