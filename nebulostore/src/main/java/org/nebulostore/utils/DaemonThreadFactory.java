package org.nebulostore.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * ThreadFactory which creates daemon threads.
 */
public class DaemonThreadFactory implements ThreadFactory {
  private static DaemonThreadFactory staticInstance_ = new DaemonThreadFactory();

  public static synchronized DaemonThreadFactory getThreadFactory() {
    return staticInstance_;
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread thread = Executors.defaultThreadFactory().newThread(runnable);
    thread.setDaemon(true);
    return thread;
  }

}
