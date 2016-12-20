package org.nebulostore.appcore.modules;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Returning job module with communication in the middle. Returns semi-result and waits for an
 * answer from the user. When the answer is provided, runs performSecondPhase(answer) implemented
 * in subclass which should finish with either endWithSuccess or endWithError.
 * In case of an error in the semi-result phase, the final result is also set to an error.
 *
 * @author szymonmatejczyk
 *
 * @param <R> Final result type.
 * @param <SR> Semi result type.
 * @param <A> Anwer result type.
 */
public abstract class TwoStepReturningJobModule<R, SR, A> extends ReturningJobModule<R> {
  private SR semiResult_;
  private NebuloException semiError_;
  private Semaphore semiResultReady_;
  private boolean afterSemiResult_;

  public TwoStepReturningJobModule() {
    semiResultReady_ = new Semaphore(0);
  }

  /**
   * Sets semiResult_. After that, module should wait on getAnswer() for a user's answer.
   *
   * @param semiResult
   */
  protected void returnSemiResult(SR semiResult) {
    semiResult_ = semiResult;
    semiResultReady_.release();
  }

  protected void returnSemiError(NebuloException error) {
    semiError_ = error;
    semiResultReady_.release();
  }

  /**
   * Blocking - user waits for SemiResult returned by module for at most timeout seconds.
   * Afterwards, user should provide an answer to semi-result via setAnswer().
   *
   * @param timeout
   * @return semi result
   * @throws NebuloException
   */
  public SR getSemiResult(int timeout) throws NebuloException {
    try {
      if (!semiResultReady_.tryAcquire(timeout, TimeUnit.SECONDS)) {
        throw new NebuloException("Timeout");
      }
    } catch (InterruptedException exception) {
      throw new NebuloException("Interrupted", exception);
    }
    afterSemiResult_ = true;
    if (semiError_ != null) {
      throw semiError_;
    } else {
      return semiResult_;
    }
  }

  /**
   * Provide an answer to semi-result and call performSecondPhase().
   * @param answer
   */
  public void setAnswer(A answer) {
    performSecondPhase(answer);
  }

  protected abstract void performSecondPhase(A answer);

  @Override
  public R getResult(int timeoutSec) throws NebuloException {
    if (!afterSemiResult_) {
      throw new NebuloException("Waiting for second result before first.");
    }
    return super.getResult(timeoutSec);
  }

  @Override
  protected void endWithError(NebuloException error) {
    // Unblock those waiting on getSemiResult().
    returnSemiError(error);
    super.endWithError(error);
  }
}
