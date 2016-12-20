package org.nebulostore.timer;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Wrapper for java.util.Timer.
 *
 * @author Bolek Kulbabinski
 */
public class TimerImpl implements Timer {
  private static Logger logger_ = Logger.getLogger(TimerImpl.class);

  private final BlockingQueue<Message> dispatcherQueue_;
  private final java.util.Timer javaTimer_;
  private final Map<String, TimerTask> tasks_ = new HashMap<>();

  @Inject
  public TimerImpl(@Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    dispatcherQueue_ = dispatcherQueue;
    javaTimer_ = new java.util.Timer(true);
  }

  @Override
  public String schedule(String jobId, long delayMillis) {
    return schedule(jobId, delayMillis, null);
  }

  @Override
  public String schedule(String jobId, long delayMillis, Object messageContent) {
    return schedule(jobId, delayMillis, messageContent, true);
  }

  @Override
  public String schedule(String jobId, long delayMillis, Object messageContent, boolean cancel) {
    return runTimerTask(new DispatcherForwardingTimeoutTimerTask(jobId, messageContent, cancel),
        delayMillis);
  }

  @Override
  public String schedule(Message message, long delayMillis) {
    return runTimerTask(new DispatcherForwardingTimerTask(message), delayMillis);
  }

  @Override
  public void scheduleRepeated(Message message, long delayMillis, long periodMillis) {
    javaTimer_.scheduleAtFixedRate(new DispatcherForwardingTimerTask(message), delayMillis,
        periodMillis);
  }

  @Override
  public void scheduleRepeatedJob(Provider< ? extends JobModule> provider, long delayMillis,
      long periodMillis) {
    javaTimer_.scheduleAtFixedRate(new DispatcherGeneratingTimerTask(provider), delayMillis,
        periodMillis);
  }

  @Override
  public void cancelTask(String taskId) {
    logger_.debug("Cancelling task " + taskId + ", current tasks: " + tasks_);
    tasks_.remove(taskId).cancel();
  }

  @Override
  public void cancelTimer() {
    logger_.debug("Cancelling the timer");
    javaTimer_.cancel();
    logger_.debug("Timer cancelled");
  }

  private String runTimerTask(TimerTask task, long delayMillis) {
    String taskId = CryptoUtils.getRandomString();
    tasks_.put(taskId, task);
    javaTimer_.schedule(task, delayMillis);
    logger_.debug("Scheduled task with id: " + taskId + ". Current tasks: " + tasks_);
    return taskId;
  }

  /**
   * @author Bolek Kulbabinski
   */
  private class DispatcherForwardingTimerTask extends TimerTask {
    private final Message message_;

    public DispatcherForwardingTimerTask(Message message) {
      message_ = message;
    }

    @Override
    public void run() {
      dispatcherQueue_.add(message_);
    }
  }

  /**
   * @author Piotr Malicki
   */
  private class DispatcherForwardingTimeoutTimerTask extends TimerTask {
    private final Message message_;
    private final boolean cancel_;

    public DispatcherForwardingTimeoutTimerTask(String jobId, Object messageContent,
        boolean cancel) {
      message_ = new TimeoutMessage(jobId, messageContent);
      cancel_ = cancel;
    }

    @Override
    public void run() {
      dispatcherQueue_.add(message_);
      if (cancel_) {
        javaTimer_.cancel();
      }
    }
  }

  /**
   * @author Bolek Kulbabinski
   */
  private class DispatcherGeneratingTimerTask extends TimerTask {
    private final Provider< ? extends JobModule> provider_;

    public DispatcherGeneratingTimerTask(Provider< ? extends JobModule> provider) {
      provider_ = provider;
    }

    @Override
    public void run() {
      dispatcherQueue_.add(new JobInitMessage(provider_.get()));
    }
  }
}
