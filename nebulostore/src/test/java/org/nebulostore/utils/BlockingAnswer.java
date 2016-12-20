package org.nebulostore.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This answer blocks waiting for an interrupt.
 *
 * @author Grzegorz Milka
 *
 * @param <T>
 */
public class BlockingAnswer<T> implements Answer<T> {
  private boolean hasAnswered_;
  private final BlockingQueue<Boolean> queue_;

  public BlockingAnswer() {
    queue_ = new LinkedBlockingQueue<>();
  }

  @Override
  public synchronized T answer(InvocationOnMock invocation) throws InterruptedException {
    if (hasAnswered_) {
      throw new IllegalStateException("BlockingAnswer has already been called.");
    }
    try {
      queue_.add(true);
      this.wait();
      throw new RuntimeException("This answer should never end correctly.");
    } finally {
      hasAnswered_ = true;
    }
  }

  public void waitForCall() throws InterruptedException {
    queue_.take();
    queue_.add(true);
  }

}
