package org.nebulostore.subscription.aop;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.nebulostore.subscription.api.SubscriptionNotificationHandler;

/**
 * Author: rafalhryciuk.
 */
@Aspect
public class SubscriptionNotificationLogger {

  private static Logger logger_ = Logger.getLogger(SubscriptionNotificationHandler.class);


  @Pointcut("execution(" +
      "* org.nebulostore.subscription.api.SubscriptionNotificationHandler" +
      " .handleSubscriptionNotification(..))")
  private void handleSubscriptionNotification() {
  }

  @After("handleSubscriptionNotification()")
  public void logNotificationHandlingBeingSuccessfullyMade(JoinPoint joinPoint) {
    logJoinPoint(joinPoint, "AFTER");
  }

  @Before("handleSubscriptionNotification()")
  public void logBeforeNotificationHandling(JoinPoint joinPoint) {
    logJoinPoint(joinPoint, "BEFORE");
  }

  @Around("handleSubscriptionNotification()")
  public Object logAroundNotificationHandling(ProceedingJoinPoint joinPoint) throws Throwable {
    logger_.info("__AROUND_BEFORE_EXECUTING__");
    Object result = joinPoint.proceed();
    logger_.info("__AROUND_AFTER_EXECUTING__ with result " + result);
    return result;
  }

  private void logJoinPoint(JoinPoint joinPoint, String method) {
    logger_.info("______ASPECT " + method + " NOTIFICATION HANDLING______");
    logger_.info("Arguments : " + Arrays.toString(joinPoint.getArgs()));
    logger_.info("Signature : " + joinPoint.getSignature());
    logger_.info("This object: " + joinPoint.getThis());
    logger_.info("Target object: " + joinPoint.getTarget());
  }

}
