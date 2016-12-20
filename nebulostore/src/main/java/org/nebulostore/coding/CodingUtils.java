package org.nebulostore.coding;


public final class CodingUtils {

  private CodingUtils() { };

  public static int[] toIntArray(byte[] array) {
    int[] data = new int[array.length];
    for (int i = 0; i < array.length; i++) {
      data[i] = array[i] & 0xFF;
    }
    return data;
  }

  public static byte[] toByteArray(int[] array) {
    byte[] data = new byte[array.length];
    for (int i = 0; i < array.length; i++) {
      data[i] = (byte) array[i];
    }
    return data;
  }

}
