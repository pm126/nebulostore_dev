package org.nebulostore.dfuntest.coding.messages;

import java.util.Map;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.dfuntest.coding.CodingTestHelperMessageForwarder;

public class RecreateObjectFragmentsStatisticsMessage extends Message {

  private static final long serialVersionUID = -6377084357545351938L;

  private final Map<ObjectId, Double> sizes_;
  private final long time_;
  private final int numberOfObjects_;

  public RecreateObjectFragmentsStatisticsMessage(Map<ObjectId, Double> sizes, long time,
      int numberOfObjects) {
    sizes_ = sizes;
    time_ = time;
    numberOfObjects_ = numberOfObjects;
  }

  public Map<ObjectId, Double> getSizes() {
    return sizes_;
  }

  public long getTime() {
    return time_;
  }

  public int getNumberOfObjects() {
    return numberOfObjects_;
  }

  @Override
  public JobModule getHandler() {
    return new CodingTestHelperMessageForwarder(this);
  }
}
