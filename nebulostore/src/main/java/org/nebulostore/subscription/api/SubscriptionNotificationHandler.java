package org.nebulostore.subscription.api;

import org.nebulostore.subscription.model.SubscriptionNotification;

/**
 * Author: rafalhryciuk.
 */
public interface SubscriptionNotificationHandler {

  void handleSubscriptionNotification(SubscriptionNotification notification);

}
