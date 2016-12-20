import java.io.*;
import java.net.*;


/**
 * Usage: FirewallTestServer [LISTENING_PORT]
 *
 * Listens for incoming UDP connection and responds. After accepting a UDP
 * message and before responding it sets up TCP server for incoming connection
 * and responds to that to. Then it tests the bandwidth by echoing received data.
 */
class FirewallTestServer {
  private static int LISTENING_PORT = 9876;
  private static int MAX_DATA_LEN = 1024;
  private static int BANDWIDTH_CHUNK_LEN = 50 * 1024;

  public static void main(String args[]) throws Exception {
    int serverPort = LISTENING_PORT;
    if (args.length > 0) {
      try {
        serverPort = Integer.parseInt(args[0]);
      } catch (NumberFormatException e) {
        System.out.println("Given arg is not a correct port number.");
      }
    }

    DatagramSocket serverDataSocket = null;
    ServerSocket tcpServerSocket = null;
    try {
      serverDataSocket = new DatagramSocket(serverPort);
    } catch (SocketException e) {
      System.out.println("UDP Socket at port: " + serverPort + " could not be opened: " + e);
      System.exit(1);
    }

    try {
      byte[] receiveData = new byte[MAX_DATA_LEN];
      byte[] sendData  = new byte[MAX_DATA_LEN];

      receiveData = new byte[MAX_DATA_LEN];

      DatagramPacket receivePacket =
        new DatagramPacket(receiveData, receiveData.length);

      System.out.println("Waiting for datagram packet");
      serverDataSocket.receive(receivePacket);

      String sentence = new String(receivePacket.getData());

      InetAddress IPAddress = receivePacket.getAddress();
      int port = receivePacket.getPort();

      System.out.println("From: " + IPAddress + ":" + port);
      System.out.println("Message: " + sentence);

      DatagramPacket sendPacket =
        new DatagramPacket(receivePacket.getData(),
            receivePacket.getData().length, IPAddress, port);

      tcpServerSocket = setUpTCPListener(serverPort);

      serverDataSocket.send(sendPacket);

      listenOnTCP(tcpServerSocket);

      bandwidthCheckListener(tcpServerSocket);
    } catch (SocketException ex) {
      System.out.println("Exception: " + ex);
      System.exit(1);
    } finally {
      if (tcpServerSocket != null) {
        tcpServerSocket.close();
      }
      serverDataSocket.close();
    }
  }

  static ServerSocket setUpTCPListener(int serverPort) throws Exception {
    ServerSocket serverSocket = null;

    try {
      serverSocket = new ServerSocket(serverPort);
    } catch (IOException e) {
      System.err.println("Could not listen on TCP port: " + serverPort + ". Due to: " + e);
      System.exit(1);
    }

    return serverSocket;
  }

  static void listenOnTCP(ServerSocket serverSocket)  throws Exception {
    Socket clientSocket = null;
    System.out.println("Waiting for connection.....");

    try {
      clientSocket = serverSocket.accept();
    } catch (IOException e) {
      System.err.println("Accept failed.");
      System.exit(1);
    }

    try {
      System.out.println("Connection successful");
      System.out.println("Waiting for input.....");

      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
          true);
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      String inputLine = in.readLine();
      if (inputLine == null) {
        System.err.println("Received null inputline");
        System.exit(1);
      }

      System.out.println("Server: " + inputLine);
      out.println(inputLine);

      in.close();
      out.close();
    } finally {
      clientSocket.close();
    }
  }

  static void bandwidthCheckListener(ServerSocket serverSocket) throws Exception {
    Socket clientSocket = null;
    System.out.println("Waiting for connection.....");

    try {
      clientSocket = serverSocket.accept();
    } catch (IOException e) {
      System.err.println("Accept failed.");
      System.exit(1);
    }

    try {
      System.out.println("Connection successful");
      System.out.println("Waiting for input.....");

      InputStream in = new BufferedInputStream(clientSocket.getInputStream());
      OutputStream out = new BufferedOutputStream(clientSocket.getOutputStream());
      byte[] buffer = new byte[BANDWIDTH_CHUNK_LEN];
      int lenRead = 0;

      while (lenRead != -1) {
        lenRead = in.read(buffer);
        if (lenRead > 0) {
          out.write(buffer, 0, lenRead);
        }
        out.flush();
      }
      in.close();
      out.close();
    } finally {
      clientSocket.close();
    }
  }

}

