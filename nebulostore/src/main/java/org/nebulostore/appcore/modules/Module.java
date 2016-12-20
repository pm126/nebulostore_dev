package org.nebulostore.appcore.modules;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for all modules.
 * Module is an object that is runnable and communicates via in/out queues.
 * @author Bolek Kulbabinski
 */
public abstract class Module implements Runnable {
  private static Logger logger_ = Logger.getLogger(Module.class);

  protected BlockingQueue<Message> inQueue_;
  protected BlockingQueue<Message> outQueue_;
  private AtomicBoolean isFinished_ = new AtomicBoolean(false);

  public Module() { }

  public Module(BlockingQueue<Message> inQueue, BlockingQueue<Message> outQueue) {
    outQueue_ = outQueue;
    inQueue_ = inQueue;
  }

  public BlockingQueue<Message> getInQueue() {
    return inQueue_;
  }

  public void setInQueue(BlockingQueue<Message> inQueue) {
    inQueue_ = inQueue;
  }

  public void setOutQueue(BlockingQueue<Message> outQueue) {
    outQueue_ = outQueue;
  }

  /**
   * Call from subclass to finish this module's thread execution.
   * TODO(bolek): Do we need to send one last message or interrupt the queue here?
   */
  protected final void endModule() {
    isFinished_.set(true);
  }

  @Override
  public void run() {
    checkNotNull(inQueue_);
    initModule();
    while (!isFinished_.get()) {
      try {
        processMessage(inQueue_.take());
        // If isFinished_ is set now, the thread will die.
      } catch (InterruptedException exception) {
        logger_.warn("Received InterruptedException from inQueue.", exception);
        continue;
      } catch (NebuloException exception) {
        logger_.error("Received NebuloException from processMessage(): " + exception.getMessage(),
            exception);
        continue;
      }
    }
  }

  /**
   * Override it to handle thread initialization.
   */
  protected void initModule() { }

  protected abstract void processMessage(Message message) throws NebuloException;

}
