package org.nebulostore.subscription.model;

import java.io.Serializable;

import org.nebulostore.appcore.addressing.NebuloAddress;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Author: rh277703.
 */
public class SubscriptionNotification implements Serializable {
  private static final long serialVersionUID = -8189822593323832329L;

  private final NebuloAddress subscribedFileAddress_;

  private final NotificationReason notificationReason_;


  public SubscriptionNotification(NebuloAddress subscribedFileAddress,
                                  NotificationReason notificationReason) {
    this.subscribedFileAddress_ = checkNotNull(subscribedFileAddress);
    this.notificationReason_ = checkNotNull(notificationReason);
  }

  public NebuloAddress getSubscribedFileAddress() {
    return subscribedFileAddress_;
  }

  public NotificationReason getNotificationReason() {
    return notificationReason_;
  }

  @Override
  public String toString() {
    return "SubscriptionNotification{" +
        "subscribedFileAddress_=" + subscribedFileAddress_ +
        ", notificationReason_=" + notificationReason_ +
        '}';
  }

  /**
   * Information about the reason of the notification.
   */
  public static enum NotificationReason {
    FILE_CHANGED, FILE_DELETED;
  }

}
