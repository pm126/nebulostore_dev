package org.nebulostore.coding;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import com.google.zxing.common.reedsolomon.GenericGF;
import com.google.zxing.common.reedsolomon.ReedSolomonDecoder;
import com.google.zxing.common.reedsolomon.ReedSolomonEncoder;
import com.google.zxing.common.reedsolomon.ReedSolomonException;

import org.junit.Test;
import org.nebulostore.crypto.CryptoUtils;

import static org.junit.Assert.assertEquals;

public class ReedSolomonTest {

  public static final int ERROR_CORRECTING_BYTES_NUMBER = 3;
  public static final GenericGF GALOIS_FIELD = GenericGF.AZTEC_DATA_8;
  public static final ReedSolomonEncoder ENCODER = new ReedSolomonEncoder(GALOIS_FIELD);
  public static final ReedSolomonDecoder DECODER = new ReedSolomonDecoder(GALOIS_FIELD);

  private int[] messageToBytes(String message) {
    int[] data = new int[message.length() + ERROR_CORRECTING_BYTES_NUMBER];
    byte[] messageBytes = message.getBytes();
    for (int i = 0; i < message.length(); i++) {
      data[i] = messageBytes[i] & 0xFF;
    }
    return data;
  }

  private String messageFromBytes(int[] data) {
    int messageLength = data.length - ERROR_CORRECTING_BYTES_NUMBER;
    byte[] messageBytes = new byte[messageLength];
    for (int i = 0; i < messageLength; i++) {
      messageBytes[i] = (byte) data[i];
    }

    return new String(messageBytes);
  }

  @Test
  public void shouldEncodeAndDecodeStringProperly() throws ReedSolomonException {
    String message = CryptoUtils.getRandomString();
    int[] data = messageToBytes(message);
    ENCODER.encode(data, ERROR_CORRECTING_BYTES_NUMBER);
    DECODER.decode(data, ERROR_CORRECTING_BYTES_NUMBER, new int[] {});
    assertEquals(message, messageFromBytes(data));
  }

  @Test
  public void shouldDecodeBrokenString() throws ReedSolomonException {
    String message = CryptoUtils.getRandomString();
    int[] data = messageToBytes(message);

    System.out.println("Message: " + Arrays.toString(data));
    ENCODER.encode(data, ERROR_CORRECTING_BYTES_NUMBER);
    System.out.println("Encoded message: " + Arrays.toString(data));
    Random rand = new Random();
    Set<Integer> positions = new HashSet<>();
    while (positions.size() < ERROR_CORRECTING_BYTES_NUMBER) {
      positions.add(rand.nextInt(data.length));
    }

    for (Integer position : positions) {
      //Erase some positions
      data[position] = 0;
    }

    int[] posArray = new int[ERROR_CORRECTING_BYTES_NUMBER];
    Iterator<Integer> iterator = positions.iterator();
    for (int i = 0; i < ERROR_CORRECTING_BYTES_NUMBER; i++) {
      posArray[i] = iterator.next();
    }

    System.out.println("Lost positions: " + Arrays.toString(posArray));

    DECODER.decode(data, ERROR_CORRECTING_BYTES_NUMBER, posArray);
    System.out.println(Arrays.toString(data));
    assertEquals(message, messageFromBytes(data));
  }
}
