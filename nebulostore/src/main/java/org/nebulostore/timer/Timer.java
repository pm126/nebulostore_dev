package org.nebulostore.timer;

import com.google.inject.Provider;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

/**
 * Timer that is able to send timeout messages via Dispatcher queue.
 * @author Bolek Kulbabinski
 */
public interface Timer {
  /**
   * Schedules a TimeoutMessage with null content to be sent to module with {@code jobId} after
   * {@code delayMillis}.
   * @param jobId ID of JobModule that is going to receive TimeoutMessage
   * @param delayMillis
   * @return Identifier of the scheduled task
   * @throws IllegalStateException if the timer is cancelled.
   */
  String schedule(String jobId, long delayMillis);

  /**
   * Schedules a TimeoutMessage with content {@code messageContent} to be sent to module with
   * {@code jobId} after {@code delayMillis}. After sending the messsage timer is automatically
   * cancelled.
   * @param jobId ID of JobModule that is going to receive TimeoutMessage
   * @param delayMillis
   * @param messageContent
   * @return Identifier of the scheduled task
   * @throws IllegalStateException if the timer is cancelled.
   */
  String schedule(String jobId, long delayMillis, Object messageContent);

  /**
   * Schedules a TimeoutMessage with content {@code messageContent} to be sent to module with
   * {@code jobId} after {@code delayMillis}.
   * @param jobId ID of JobModule that is going to receive TimeoutMessage
   * @param delayMillis
   * @param messageContent
   * @param cancel boolean value indicating if the timer has to be cancelled after sending
   * the message
   * @return Identifier of the scheduled task
   * @throws IllegalStateException if the timer is cancelled.
   */
  String schedule(String jobId, long delayMillis, Object messageContent, boolean cancel);

  /**
   * Schedules {@code message} to be sent to dispatcher after {@code delayMillis}.
   * @param message
   * @param delayMillis
   * @return Identifier of the scheduled task
   * @throws IllegalStateException if the timer is cancelled.
   */
  String schedule(Message message, long delayMillis);

  /**
   * Schedules {@code message} to be sent to dispatcher after {@code delayMillis} and then
   * repeatedly every {@code periodMillis}.
   * @param message
   * @param delayMillis
   * @param periodMillis
   * @throws IllegalStateException if the timer is cancelled.
   */
  void scheduleRepeated(Message message, long delayMillis, long periodMillis);

  /**
   * Schedules {@code JobInitMessage} containing module provided by {@code provider} to be sent to
   * dispatcher after {@code delayMillis} and then repeatedly every {@code periodMillis}.
   * @param provider
   * @param delayMillis
   * @param periodMillis
   * @throws IllegalStateException if the timer is cancelled.
   */
  void scheduleRepeatedJob(Provider< ? extends JobModule> provider, long delayMillis,
      long periodMillis);

  /**
   * Cancels a task with given identifier.
   */
  void cancelTask(String taskId);

  /**
   * Cancels all scheduled tasks on this timer.
   */
  void cancelTimer();
}
