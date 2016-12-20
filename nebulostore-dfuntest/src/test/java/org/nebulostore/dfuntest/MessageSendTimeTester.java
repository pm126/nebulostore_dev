package org.nebulostore.dfuntest;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.nebulostore.communication.naming.CommAddress;

public class MessageSendTimeTester {

  private static final int NUMBER_OF_PEERS = 50;
  private static final int MIN_WAITING_TIME_MILIS = 15000;
  private static final int MAX_WAITING_TIME_MILIS = 60000;
  private static final int NUMBER_OF_PEERS_TO_SELECT = 15;
  private static final long SIMULATION_LENGTH_MILIS = 5 * 60 * 60 * 1000;

  @Test
  public void test() {
    List<CommAddress> peers = new ArrayList<>();
    for (int i = 1; i < NUMBER_OF_PEERS; i++) {
      peers.add(CommAddress.newRandomCommAddress());
    }

    int currentTime = 0;
    Map<CommAddress, Integer> counters = new HashMap<>();
    while (currentTime < SIMULATION_LENGTH_MILIS) {
      Collections.shuffle(peers);
      for (int i = 0; i < NUMBER_OF_PEERS_TO_SELECT; i++) {
        System.out.println("Time " + new Date(currentTime) + ": peer " + peers.get(i).getPeerId());
        if (!counters.containsKey(peers.get(i))) {
          counters.put(peers.get(i), 0);
        }
        counters.put(peers.get(i), counters.get(peers.get(i)) + 1);
      }

      Random random = new Random();
      currentTime += random.nextInt(MAX_WAITING_TIME_MILIS - MIN_WAITING_TIME_MILIS);
    }

    for (Entry<CommAddress, Integer> entry : counters.entrySet()) {
      System.out.println("Peer " + entry.getKey() + ": " + entry.getValue());
    }
  }

  @Test
  public static void main(String[] args) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    System.out.println("Running rest module " + (System.currentTimeMillis() - startTime));
    URI uri = URI.create(String.format("%s:%s/", "http://0.0.0.0", "11001"));
    System.out.println("URI created " + (System.currentTimeMillis() - startTime));
    ResourceConfig config = new ResourceConfig();
    System.out.println("Server configured " + (System.currentTimeMillis() - startTime));
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, config);
    System.out.println("Http server created " + (System.currentTimeMillis() - startTime));
    //httpServer.start();
    Thread.sleep(60000);
    //httpServer.start();
  }

}
