package org.nebulostore.dfuntest.comm.peers;

import com.google.inject.Inject;

import org.nebulostore.dfuntest.comm.CommPerfTestHelperModule;
import org.nebulostore.peers.Peer;

public class CommPerfTestingPeer extends Peer {

  private CommPerfTestHelperModule commPerfTestHelper_;

  @Inject
  public void setCommPerfTestHelperModule(CommPerfTestHelperModule commPerfTestHelper) {
    commPerfTestHelper_ = commPerfTestHelper;
  }

  @Override
  protected void initializeModules() {
    commPerfTestHelper_.runThroughDispatcher();
    //msgReceivingCheckerThread_.start();
    //runNetworkMonitor();
    //runBroker();
    //runAsyncMessaging();
  }

  @Override
  public void quitNebuloStore() {
    commPerfTestHelper_.shutDown();
    super.quitNebuloStore();
  }
}
