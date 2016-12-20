package org.nebulostore.dfuntest.coding.api;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.api.WriteNebuloObjectPartsModule;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.timer.Timer;

public class WriteNebuloObjectPartsWithSize extends WriteNebuloObjectPartsModule {

  @Inject
  public WriteNebuloObjectPartsWithSize(EncryptionAPI encryption, Timer timer) {
    super(encryption, timer);
  }

  @Inject
  public void setTempNetworkQueue(@Named("CalculatorInQueue") BlockingQueue<Message> networkQueue) {
    networkQueue_ = networkQueue;
  }

}
