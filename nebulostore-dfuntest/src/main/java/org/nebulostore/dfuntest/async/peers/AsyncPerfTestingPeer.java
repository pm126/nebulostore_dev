package org.nebulostore.dfuntest.async.peers;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.dfuntest.async.AsyncTestHelperModule;
import org.nebulostore.peers.Peer;

/**
 * Peer for asynchronous messages performance test.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncPerfTestingPeer extends Peer {
  private static Logger logger_ = Logger.getLogger(AsyncPerfTestingPeer.class);

  private AsyncTestHelperModule asyncTestHelper_;

  @Inject
  public void setAsyncTestHelperModule(AsyncTestHelperModule asyncTestHelper) {
    logger_.debug("Setting helper");
    asyncTestHelper_ = asyncTestHelper;
  }

  @Override
  protected void initializeModules() {
    logger_.debug("initModules");
    super.initializeModules();
    runAsyncTestHelper();
  }

  private void runAsyncTestHelper() {
    logger_.debug("Starting helper");
    asyncTestHelper_.runThroughDispatcher();
  }

  @Override
  public void quitNebuloStore() {
    asyncTestHelper_.shutDown();
    super.quitNebuloStore();
  }

}
