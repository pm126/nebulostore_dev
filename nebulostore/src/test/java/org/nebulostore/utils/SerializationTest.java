package org.nebulostore.utils;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.async.synchronization.VectorClockValue;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

  @Test
  public void sizeTest() {
    System.out.println(SerializationUtils.serialize(new TestMessage(null, null)).length);
  }

  @Test
  public void stringSizeTest() {
    System.out.println(RandomStringUtils.random(1024).getBytes().length);
  }

  @Test
  public void queuePerfTest() throws InterruptedException {
    final Logger logger = Logger.getLogger(SerializationTest.class);
    final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < 880; i++) {
      queue.add(new TestMessage(CommAddress.getZero(), CommAddress.getZero()));
    }

    Runnable thread = new Runnable() {

      private final Map<Message, Integer> map_ = new HashMap<>();
      private final Map<Message, String> map2_ = new HashMap<>();

      @Override
      public void run() {
        long timestamp = System.currentTimeMillis();
        while (!queue.isEmpty()) {
          logger.info("Test");
          try {
            Message message = queue.take();
            visit(message);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        System.out.println(System.currentTimeMillis() - timestamp);
      }

      public void visit(Message m) {
        //String s = "New message received: " + m + " jobId_ " + m.getId() + " id_ " +
        //    m.getMessageId();
        map_.put(m, new Integer(1));
        map2_.put(m, "as");
        updateMsg(m);
      }

      public void updateMsg(Message m) {
        if (queue.isEmpty()) {
          map_.put(m, new Integer(1));
        }
      }
    };

    Thread t = new Thread(thread);
    t.start();
    t.join();
  }

  @Test
  public void jsonTest() {

    Map<ObjectId, Integer> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      ObjectId objectId = new ObjectId(BigInteger.valueOf(i*5));
      map.put(objectId, i*10);
    }

    GsonBuilder builder = new GsonBuilder().enableComplexMapKeySerialization();
    Gson gson = builder.create();

    String result = gson.toJson(map);
    System.out.println(result);

    Type mapType = new TypeToken<Map<ObjectId, Integer>>() { }.getType();
    Map<ObjectId, Integer> map2 = new Gson().fromJson(result, mapType);
    assertEquals(map2, map);
  }

  private static class TestMessage extends CommMessage {

    private static final long serialVersionUID = 2578532863295326183L;

    private final Map<Integer, Double> map_ = new HashMap<>();
    private final Set<VectorClockValue> vectorClocks_ = new HashSet<>();

    public TestMessage(CommAddress sourceAddress, CommAddress destAddress) {
      super(sourceAddress, destAddress);
      for (int i = 0; i < 100; i++) {
        map_.put(Integer.valueOf(i), Double.valueOf(i));
      }

      for (int i = 0; i < 10; i++) {

        VectorClockValue value = new VectorClockValue(CommAddress.newRandomCommAddress(), 0);
        for (int j = 0; j < 10; j++) {
          value.updateClockValue(new VectorClockValue(CommAddress.newRandomCommAddress(), 0));
        }
        vectorClocks_.add(value);
      }
    }

    /**
     *
     */

  }

}
