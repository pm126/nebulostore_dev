package org.nebulostore.appcore.model;

import java.util.Arrays;

import org.junit.Test;
import org.nebulostore.appcore.exceptions.NebuloException;

import static org.junit.Assert.assertTrue;

/**
 * Simple unit test for NebuloFile.
 * @author Bolek Kulbabinski
 */
public final class NebuloFileTest {
  private static final String APP_KEY = "1";
  private static final String OBJECT_ID = "3";

  @Test
  public void testWriteMultipleChunks() throws NebuloException {
    NebuloFile file = NebuloObjectUtils.getNewNebuloFile(APP_KEY, OBJECT_ID);

    byte[] as = new byte[35];
    Arrays.fill(as, (byte) 'a');
    byte[] bs = new byte[38];
    Arrays.fill(bs, (byte) 'b');
    byte[] cs = new byte[13];
    Arrays.fill(cs, (byte) 'c');
    byte[] sum = new byte[53];
    System.arraycopy(as, 0, sum, 0, 35);
    System.arraycopy(bs, 0, sum, 15, 38);
    System.arraycopy(cs, 0, sum, 19, 13);

    file.write(as, 0);
    file.write(bs, 15);
    file.write(cs, 19);
    byte[] check = file.read(0, file.getSize());

    assertTrue(file.getSize() == 53);
    assertTrue(check.length == 53);
    assertTrue(Arrays.equals(check, sum));
  }
}
