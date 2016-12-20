package org.nebulostore.dispatcher;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Injector;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.appcore.modules.Module;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dispatcher - core module that assigns threads to tasks and distributes messages
 *     among existing job modules.
 */
public class Dispatcher extends Module {
  private static Logger logger_ = Logger.getLogger(Dispatcher.class);
  private static final int JOIN_TIMEOUT_MILLIS = 3000;

  private final Map<String, BlockingQueue<Message>> workersQueues_;
  private final Map<String, Thread> workersThreads_;
  private final MessageVisitor visitor_;
  private final Injector injector_;

  /**
   * Visitor class. Contains logic for handling messages depending
   * on their types.
   */
  public class MessageDispatchVisitor extends MessageVisitor {
    private static final int MAX_LOGGED_JOB_ID_LENGTH = 8;

    /*
     * Special handling for JobEndedMessage.
     * Remove MSG_ID from Dispatcher's map.
     */
    public void visit(JobEndedMessage message) {
      if (message.getId() != null) {
        String jobId = message.getId();
        logger_.info("Got job ended message with ID: " + jobId);
        if (workersQueues_.containsKey(jobId)) {
          workersQueues_.remove(jobId);
        }
        if (workersThreads_.containsKey(jobId)) {
          try {
            workersThreads_.get(jobId).join(JOIN_TIMEOUT_MILLIS);
          } catch (InterruptedException e) {
            logger_.warn("InterruptedException while waiting for a dying thread to join. JobID = " +
                jobId);
          }
          workersThreads_.remove(jobId);
        }
      } else {
        logger_.debug("Got job ended message with NULL ID.");
      }
    }

    /*
     * End dispatcher.
     */
    public void visit(EndModuleMessage message) throws NebuloException {
      Thread[] threads =
          workersThreads_.values().toArray(new Thread[workersThreads_.values().size()]);
      logger_.debug("Quitting dispatcher, waiting for " + threads.length + " job threads.");
      //FIXME śmietnik
      try {
        Thread.sleep(JOIN_TIMEOUT_MILLIS * 10);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      /*for (int i = 0; i < threads.length; ++i) {
        try {
          logger_.debug("Waiting for thread: " + threads[i].toString());
          threads[i].join(JOIN_TIMEOUT_MILLIS);
        } catch (InterruptedException exception) {
          logger_.debug("Thread " + threads[i] + " state: " + threads[i].isAlive());
          continue;
        }
        logger_.debug("Thread " + threads[i] + " state: " + threads[i].isAlive());
      }*/
      endModule();
    }

    /*
     * General behavior - forwarding messages.
     */
    @Override
    public void visitDefault(Message message) throws NebuloException {
      if (message.getId() != null) {
        String jobId = message.getId();
        logger_.info("Received message with jobID: " + jobId + " and class name: " +
            message.getClass().getSimpleName());
        //logger_.debug("Message: " + message);
        logger_.debug("inQueue size: " + inQueue_.size());
        logger_.debug("Number of threads: " + workersThreads_.size());

        // TODO(bolek): Maybe move injection to deserialization process?
        injector_.injectMembers(message);
        logger_.debug("Message injected");
        if (!workersQueues_.containsKey(jobId)) {
          // Spawn a new thread to handle the message.
          try {
            JobModule handler = message.getHandler();
            BlockingQueue<Message> newInQueue = new LinkedBlockingQueue<Message>();
            handler.setInQueue(newInQueue);
            handler.setJobId(jobId);
            logger_.debug("Handler prepared");
            // TODO(bolek): Remove this. Message injection should be sufficient.
            injector_.injectMembers(handler);
            logger_.debug("Handler injected");
            newInQueue.add(message);
            if (handler.isQuickNonBlockingTask()) {
              logger_.debug("Executing in current thread with handler of type " +
                  handler.getClass().getSimpleName());
              handler.run();
              logger_.debug("Handler executed");
            } else {
              logger_.debug("Creating new thread");
              Thread newThread = new Thread(handler, handler.getClass().getSimpleName() + ":" +
                  jobId.substring(0, Math.min(MAX_LOGGED_JOB_ID_LENGTH, jobId.length())));
              logger_.debug("Thread created");
              newThread.setDaemon(true);
              workersQueues_.put(jobId, newInQueue);
              workersThreads_.put(jobId, newThread);
              logger_.debug("Data structures updated");
              logger_.info("Starting new thread with handler of type " +
                  handler.getClass().getSimpleName());
              newThread.start();
              logger_.debug("Thread started");
            }
          } catch (NebuloException exception) {
            logger_.debug("Message does not contain a handler.");
          }
        } else {
          logger_.debug("Delegating message to an existing worker thread.");
          workersQueues_.get(jobId).add(message);
        }
      } else {
        logger_.debug("Received message with NULL jobID and class name: " +
            message.getClass().getSimpleName());
      }
    }
  }

  /**
   *
   * @param inQueue Input message queue.
   * @param outQueue Output message queue, which is also later passed to newly
   *                 created tasks (usually network's inQueue).
   */
  public Dispatcher(BlockingQueue<Message> inQueue,
      BlockingQueue<Message> outQueue, Injector injector) {
    super(inQueue, outQueue);
    visitor_ = new MessageDispatchVisitor();
    workersQueues_ = new TreeMap<String, BlockingQueue<Message>>();
    workersThreads_ = new TreeMap<String, Thread>();
    injector_ = checkNotNull(injector);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
