package org.nebulostore.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ForHangTest {

  @Test
  public void test() throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(new Runnable() {

      @Override
      public void run() {
        int i = 0;
        while (true) {
          i++;
        }
      }

    });

    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  private long fibonacci(long number) {
    if (number < 2) {
      return 1L;
    }
    return fibonacci(number - 1) + fibonacci(number - 2);
  }

}
