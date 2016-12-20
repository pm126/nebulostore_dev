package org.nebulostore.dfuntest.coding.peers;

import com.google.inject.Inject;

import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.dfuntest.coding.CodingTestHelperModule;
import org.nebulostore.dfuntest.coding.SentDataSizeCalculatorModule;
import org.nebulostore.dfuntest.perfchecker.PerformanceCheckerModule;
import org.nebulostore.peers.Peer;

public class CodingAvailabilityTestingPeer extends Peer {

  private CodingTestHelperModule helper_;
  private PerformanceCheckerModule checker_;
  private SentDataSizeCalculatorModule calculator_;
  private Thread calculatorThread_;

  @Inject
  public void setCodingTestHelperModule(CodingTestHelperModule helper) {
    helper_ = helper;
  }

  @Inject
  public void setPerfCheckerModule(PerformanceCheckerModule checker) {
    checker_ = checker;
  }

  @Inject
  public void setSentDataSizeCalculatorModule(SentDataSizeCalculatorModule calculator) {
    calculator_ = calculator;
  }

  @Override
  protected void initializeModules() {
    super.initializeModules();
    helper_.runThroughDispatcher();
    checker_.runThroughDispatcher();
    calculatorThread_ = new Thread(calculator_);
    calculatorThread_.start();
  }

  @Override
  protected void cleanModules() {
    checker_.getInQueue().add(new EndModuleMessage());
    calculator_.getInQueue().add(new EndModuleMessage());
    try {
      calculatorThread_.join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
