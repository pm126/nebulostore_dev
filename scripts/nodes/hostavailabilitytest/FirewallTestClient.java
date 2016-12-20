import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Tests a given server: sends TCP, UDP packets and tests bandwidth.
 */
class FirewallTestClient {
  private static final int SERVER_PORT = 9876;
  private static final int MAX_DATA_LEN = 1024;
  private static final int BANDWIDTH_CHUNK_LEN = 50 * 1024;
  private static final int BANDWIDTH_CHECK_DATA = 1 * 1024 * 1024;
  private static final String BANDWIDTH_LOG_NAME = "bandwidth.log";

  /**
   *  Usage: FirewallTestClient SERVER_HOSTNAME [SERVER_PORT]
   *
   * Returns 0 iff client has successfully communicated with given server (TCP, UDP, bandwidth).
   */
  public static void main(String[] args) {
    int minBandwidth = 100;
    if (args.length == 0) {
      System.err.println("Incorrect arguments\n" +
              "Usage: FirewallTestClient SERVER_HOSTNAME [SERVER_PORT]" +
              " [min bandwidth kilobytes per second]");
      System.exit(1);
    }

    String hostname = args[0];
    int port = SERVER_PORT;
    if (args.length > 1) {
      try {
        port = Integer.parseInt(args[1]);
      } catch (NumberFormatException e) {
        System.out.println("Given arg is not a correct port number.");
        System.exit(1);
      }
    }
    if (args.length > 2) {
      try {
        minBandwidth = Integer.parseInt(args[2]);
      } catch (NumberFormatException e) {
        System.out.println("Given arg is not a correct minimal bandwidth.");
        System.exit(1);
      }
    }

    sendUDPMessage(hostname, port);
    sendTCPMessage(hostname, port);
    int bandwidth = testBandwidthClient(hostname, port, minBandwidth);
    if (bandwidth < minBandwidth) {
      System.out.println("bandwidth not sufficient; computed bandwidth " +
          bandwidth + " required bandwidth: " + minBandwidth);
      System.exit(2);
    }
  }

  static void sendUDPMessage(String serverHostname, int port) {
    try {
      DatagramSocket clientSocket = new DatagramSocket();

      InetAddress ipAddress = InetAddress.getByName(serverHostname);
      System.out.println("Attemping to connect to " + ipAddress + " via UDP port " + port);

      byte[] sendData = (new String("EXAMPLE")).getBytes();
      byte[] receiveData = new byte[MAX_DATA_LEN];

      System.out.println("Sending data (" + sendData.length + " bytes) to server.");

      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, ipAddress, port);

      clientSocket.send(sendPacket);

      DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

      System.out.println("Waiting for return packet");
      clientSocket.setSoTimeout(1000);

      try {
        clientSocket.receive(receivePacket);
        String sentence = new String(receivePacket.getData());

        InetAddress returnIPAddress = receivePacket.getAddress();

        int returnPort = receivePacket.getPort();

        System.out.println("From server at: " + returnIPAddress + ":" + returnPort);
        System.out.println("Message: " + sentence);
      } catch (SocketTimeoutException ste) {
        System.out.println("Timeout Occurred: Packet assumed lost");
        System.exit(1);
      }

      clientSocket.close();
    } catch (UnknownHostException ex) {
      System.err.println(ex);
      System.exit(1);
    } catch (IOException ex) {
      System.err.println(ex);
      System.exit(1);
    }
  }

  static void sendTCPMessage(String serverHostname, int port) {
    System.out.println("Attemping to connect to host " + serverHostname + " on port " + port);

    Socket echoSocket = null;
    PrintWriter out = null;
    BufferedReader in = null;

    try {
      echoSocket = new Socket(serverHostname, port);
      out = new PrintWriter(echoSocket.getOutputStream(), true);
      in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
      out.println("EXAMPLE");
      System.out.println("echo: " + in.readLine());
      out.close();
      in.close();
      echoSocket.close();
    } catch (UnknownHostException e) {
      System.err.println("Don't know about host: " + serverHostname);
      System.exit(1);
    } catch (IOException e) {
      System.err.println("Couldn't get I/O for the connection to: " + serverHostname);
      System.exit(1);
    }
  }

  static int testBandwidthClient(String serverHostname, int port, int minBandwidth) {
    try {
      Socket clientSocket = new Socket(serverHostname, port);
      InputStream in = new BufferedInputStream(clientSocket.getInputStream());
      OutputStream out = new BufferedOutputStream(clientSocket.getOutputStream());
      byte[] buffer = new byte[BANDWIDTH_CHUNK_LEN];
      int timeOut = (BANDWIDTH_CHECK_DATA / (minBandwidth * 1024)) * 1000;
      int totalRead = 0;

      long startTime = System.currentTimeMillis();
      long expectedFinish = startTime + timeOut;
      long finishTime;
      try {
        while (totalRead < BANDWIDTH_CHECK_DATA) {
          clientSocket.setSoTimeout(timeOut);
          out.write(buffer);
          out.flush();
          int chunkRead = 0;
          while (chunkRead < BANDWIDTH_CHUNK_LEN) {
            long currTime = System.currentTimeMillis();
            clientSocket.setSoTimeout((int) (expectedFinish - currTime));
            int len = in.read(buffer, chunkRead, buffer.length - chunkRead);
            totalRead += len;
            chunkRead += len;
          }
        }
        finishTime = System.currentTimeMillis();
      } catch (SocketException e) {
        finishTime = System.currentTimeMillis();
      }
      long duration = finishTime - startTime;
      double bandwidth = (totalRead / ((double) duration / 1000)) / 1024;
      in.close();
      out.close();
      clientSocket.close();
      PrintWriter log = new PrintWriter(new FileWriter(BANDWIDTH_LOG_NAME, true));
      String timestamp = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date());
      log.println(timestamp + " " + serverHostname + " " + duration + " " +
          totalRead + " " + bandwidth);
      log.close();
      return (int) bandwidth;
    } catch (IOException e) {
      System.err.println("IO Exception while testing bandwidth");
      System.exit(1);
      return -1;
    }
  }

}
