package org.nebulostore.dfuntest.perfchecker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.dispatcher.JobInitMessage;

public class PerformanceCheckerModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(PerformanceCheckerModule.class);

  private final MessageVisitor visitor_ = new PerfCheckerVisitor();
  private final ScheduledExecutorService executor_ = Executors.newScheduledThreadPool(1);

  public class PerfCheckerVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Starting");
      executor_.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          logger_.debug("Tick");
        }

      }, 100, 100, TimeUnit.MILLISECONDS);
    }

    public void visit(EndModuleMessage message) {
      logger_.debug("Ending");
      executor_.shutdownNow();
      try {
        executor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
