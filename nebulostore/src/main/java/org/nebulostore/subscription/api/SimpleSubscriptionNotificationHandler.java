package org.nebulostore.subscription.api;

import org.nebulostore.subscription.model.SubscriptionNotification;

/**
 * Author: rafalhryciuk.
 */
public class SimpleSubscriptionNotificationHandler implements SubscriptionNotificationHandler {

  @Override
  public void handleSubscriptionNotification(SubscriptionNotification notification) {
    System.out.println("==========================================================");
    System.out.println(notification);
    System.out.println("==========================================================");
  }

}
