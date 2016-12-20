package org.nebulostore.systest.performance;

import org.nebulostore.systest.TestingPeer;

/**
 * Testing peer for performance tests. It uses OneTimeUniformGossip gossiping service to
 * quickly establish uniform replication contracts among peers. Apart from that, it behaves like
 * TestingPeer.
 *
 * @author Bolek Kulbabinski
 */
public class PerfTestingPeer extends TestingPeer {
  @Override
  protected void runBroker() {
    // No broker needed for server.
    if (!isTestServer_) {
      super.runBroker();
    }
  }
}
