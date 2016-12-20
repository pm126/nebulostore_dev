package org.nebulostore.appcore.modules;

import org.junit.Test;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for ReturningJobModule class.
 * @author Bolek Kulbabinski
 */
public final class ReturningJobModuleTest extends ReturningJobModuleBase {
  private static final int SLEEP_TIME_MILLIS = 50;

  @Test
  public void testEndWithSuccess() throws NebuloException {
    ReturningJobModule<Integer> module = new SuccessModule();
    setupModule(module);
    Integer result = module.getResult(TIMEOUT_SEC);
    assertEquals(result, SUCCESS_VALUE);
  }

  @Test
  public void testEndWithFailure() {
    ReturningJobModule<Integer> module = new ErrorModule();
    setupModule(module);
    try {
      module.getResult(TIMEOUT_SEC);
      fail();
    } catch (NebuloException exception) {
      assertEquals(exception.getMessage(), ERROR_VALUE);
    }
  }

  /**
   * Module that ends with success after a short wait.
   */
  private class SuccessModule extends ReturningJobModule<Integer> {
    @Override
    protected void processMessage(Message message) throws NebuloException {
      sleep(SLEEP_TIME_MILLIS);
      endWithSuccess(SUCCESS_VALUE);
    }
  }

  /**
   * Module that ends with error after a short wait.
   */
  private class ErrorModule extends ReturningJobModule<Integer> {
    @Override
    protected void processMessage(Message message) throws NebuloException {
      sleep(SLEEP_TIME_MILLIS);
      endWithError(new NebuloException("my_error"));
    }
  }
}
