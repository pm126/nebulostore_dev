package org.nebulostore.dfuntest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import me.gregorias.dfuntest.App;
import me.gregorias.dfuntest.CommandException;
import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.dfuntest.RemoteProcess;

import org.glassfish.jersey.client.ClientProperties;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dfuntest.async.utils.MessagesInfoDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java's proxy to Nebulostore external REST interface.
 *
 * @author Grzegorz Milka
 */
public class NebulostoreApp extends App<Environment> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NebulostoreApp.class);

  private static final int REST_CLIENT_CONNECT_TIMEOUT_MILIS = 120000;
  private static final int REST_CLIENT_READ_TIMEOUT_MILIS = 120000;

  protected final Environment env_;
  protected final URI uri_;
  private final String javaCommand_;
  private RemoteProcess process_;
  //private Thread errorRedirectorThread_;
  //private Thread stdoutRedirectorThread_;

  public NebulostoreApp(int id, String name, Environment env, URI uri, String javaCommand) {
    super(id, name);
    env_ = env;
    uri_ = uri;
    javaCommand_ = javaCommand;
  }

  @Override
  public Environment getEnvironment() {
    return env_;
  }

  public Collection<String> getPeersList() throws IOException {
    String peersList = getJsonStringByREST("network_monitor/peers_list");
    Collection<String> peersListCollection = new ArrayList<>();
    for (JsonElement element : new JsonParser().parse(peersList).getAsJsonArray()) {
      peersListCollection.add(element.getAsString());
    }
    return peersListCollection;
  }

  @Override
  public void startUp() throws CommandException, IOException {
    LOGGER.debug("[{}] startUp()", getId());
   /* if (errorRedirectorThread_ != null) {
      errorRedirectorThread_.stop();
    }
    if (stdoutRedirectorThread_ != null) {
      stdoutRedirectorThread_.stop();
    }*/
    List<String> runCommand = new LinkedList<>();

    LOGGER.debug("Preparing mkdir command");
    List<String> mkDirCommand = new LinkedList<>();
    mkDirCommand.add("mkdir");
    mkDirCommand.add("logs");
    try {
      env_.runCommand(mkDirCommand);
    } catch (InterruptedException e1) {
      LOGGER.warn("Could not start application.", e1);
      return;
    }
    LOGGER.debug("Performed mkdir command");

    LOGGER.debug("Preparing run command");
    runCommand.add(javaCommand_);
    runCommand.add("-Xms300m");
    runCommand.add("-Xmx400m");
    runCommand.add("-XX:+HeapDumpOnOutOfMemoryError");
    runCommand.add("-Xloggc:vgc" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) +
        " -XX:+PrintGCDetails -XX:+PrintGCDateStamps");
    runCommand.add(
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
    /*runCommand.add("-Dcom.sun.management.jmxremote.ssl=false " +
        "-Dcom.sun.management.jmxremote.authenticate=false " +
        "-Dcom.sun.management.jmxremote.port=" + (5998 + getId()) +
        "-Dcom.sun.management.jmxremote.rmi.port=" + (5998 + getId()));
    runCommand.add("-agentlib:hprof=file=hprof" + DateFormatUtils.format(
        new Date(), "yyyyMMddhhmmss") + ".txt,heap=dump,format=b,depth=40");*/
    runCommand.add("-jar");
    runCommand.add(NebulostoreEnvironmentPreparator.JAR_FILE);
    LOGGER.debug("Prepared run command");
    process_ = env_.runCommandAsynchronously(runCommand);
    LOGGER.debug("Started run command asynchronously");
    /*errorRedirectorThread_ = new Thread(new StreamRedirector(process_.getErrorStream(),
        new FileOutputStream(new File("error" + getId()))));
    errorRedirectorThread_.start();
    stdoutRedirectorThread_ = new Thread(new StreamRedirector(process_.getInputStream(),
        new FileOutputStream(new File("out" + getId()))));
    stdoutRedirectorThread_.start();*/
  }

  @Override
  public void shutDown() throws IOException {
    LOGGER.debug("[{}] shutDown()", getId());
    if (process_ == null) {
      //throw new IllegalStateException("Nebulostore is not running.");
      LOGGER.info("Nebulostore is not running");
      return;
    }
    //process_.destroy();

    try {
      terminate();
      try {
        LOGGER.debug("before waitFor");
        //Thread.sleep(60000);
        process_.waitFor();
        LOGGER.debug("after waitFor");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } catch (IOException e) {
      process_.destroy();
    }
    process_ = null;
  }

  private void terminate() throws IOException {
    LOGGER.debug("terminate()");
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("terminate");

    String keyStr;
    try {
      keyStr = target.request(MediaType.APPLICATION_JSON).get(String.class);
    } catch (ProcessingException e) {
      LOGGER.error("terminate()", e);
      throw new IOException("Could not get key.", e);
    }
    client.close();
    LOGGER.debug("terminate() -> void", keyStr);
    return;
  }

  public boolean isRunning() {
    LOGGER.debug("isRunning() for app " + getId());
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("peer/ping");

    try {
      target.request().get();
    } catch (ProcessingException e) {
      LOGGER.info("isRunning() -> false in app " + getId());
      return false;
    } finally {
      client.close();
    }
    return true;
  }

  public CommAddress getCommAddress() {
    return new CommAddress(String.format("00000000-0000-0000-%04d-000000000000", getId()));
  }

  public Map<CommAddress, Set<String>> getSentAsyncTestMessages() throws IOException {
    return getAsyncTestMessagesMap("async_helper/sent_messages");
  }

  public Map<CommAddress, Set<String>> getReceivedAsyncTestMessages() throws IOException {
    return getAsyncTestMessagesMap("async_helper/received_messages");
  }

  private Map<CommAddress, Set<String>> getAsyncTestMessagesMap(String uriString)
      throws IOException {
    String messagesMap = getJsonStringByREST(uriString);
    return new MessagesInfoDeserializer().apply(messagesMap);
  }

  public void startAsyncHelperMessagesSending() {
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("async_helper/start_sending");
    try {
      target.request().head();
    } catch (ProcessingException e) {
      LOGGER.warn("Could not start async helper's messages sending on peer " + getCommAddress(), e);
    } finally {
      client.close();
    }
  }

  public void stopAsyncHelperMessagesSending() {
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("async_helper/stop_sending");
    try {
      target.request().head();
    } catch (ProcessingException e) {
      LOGGER.warn("Could not stop async helper's messages sending on peer " + getCommAddress(), e);
    } finally {
      client.close();
    }
  }

  public Map<String, Long> getMessagesDelays() throws IOException {
    String delaysMap = getJsonStringByREST("async_helper/messages_delays");
    Type type = (new TypeToken<Map<String, Long>>() { }).getType();
    return new Gson().fromJson(delaysMap, type);
  }

  public void storeObjects() {
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path(
        "coding_helper/store_objects");
    try {
      target.request().head();
    } catch (ProcessingException e) {
      LOGGER.warn("Could not store objects on peer " + getCommAddress(), e);
    } finally {
      client.close();
    }
  }

  public void startReaderMode() {
    LOGGER.debug("Trying to start reader mode on peer: " + getCommAddress());
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("coding_helper/start_reader_mode");
    try {
      target.request().head();
      LOGGER.debug("Started reader mode on peer: " + getCommAddress());
    } catch (ProcessingException e) {
      LOGGER.warn("Could not start reader mode on peer " + getCommAddress(), e);
    } finally {
      client.close();
    }
  }

  public void stopReaderMode() {
    LOGGER.debug("Trying to stop reader mode on peer: " + getCommAddress());
    //FIXME copy-paste
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("coding_helper/stop_reader_mode");
    try {
      target.request().head();
      LOGGER.debug("Stopped reader mode on peer: " + getCommAddress());
    } catch (ProcessingException e) {
      LOGGER.warn("Could not stop reader mode on peer " + getCommAddress(), e);
    } finally {
      client.close();
    }
  }

  public String getCodingTestResults() throws IOException {
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("coding_helper/test_results");
    try {
      return target.request(MediaType.TEXT_PLAIN).get(String.class);
    } catch (ProcessingException e) {
      throw new IOException("Could not download coding test results.", e);
    } finally {
      client.close();
    }
  }

  public Map<CommAddress, Set<String>> getSentCommPerfTestMessages() throws IOException {
    return getCommPerfTestMessagesMap("comm_perf_helper/sent_messages");
  }

  public Map<CommAddress, Set<String>> getReceivedCommPerfTestMessages() throws IOException {
    return getCommPerfTestMessagesMap("comm_perf_helper/received_messages");
  }

  private Map<CommAddress, Set<String>> getCommPerfTestMessagesMap(String uriString)
      throws IOException {
    String messagesMap = getJsonStringByREST(uriString);
    return new MessagesInfoDeserializer().apply(messagesMap);
  }

  public void stopCommPerfHelperMessagesSending() {
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path("comm_perf_helper/stop_sending");
    try {
      target.request().head();
    } catch (ProcessingException e) {
      LOGGER.warn("Could not stop async helper's messages sending on peer " + getCommAddress(), e);
    } finally {
      client.close();
    }
  }

  /**
   * Try to prepare environment using given EnvironmentPreparator.
   *
   * @param preparator
   */
  public void tryPrepareEnvironment(EnvironmentPreparator<Environment> preparator)
      throws IOException {
    preparator.prepare(Sets.newHashSet(env_));
  }

  private String getJsonStringByREST(String uri) throws IOException {
    Client client = createNewClient();
    WebTarget target = client.target(uri_).path(uri);
    try {
      return target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    } catch (ProcessingException e) {
      throw new IOException("Could not json string by REST from uri: " + uri + ".", e);
    } finally {
      client.close();
    }
  }

  private Client createNewClient() {
    Client client = ClientBuilder.newClient();
    client.property(ClientProperties.CONNECT_TIMEOUT, REST_CLIENT_CONNECT_TIMEOUT_MILIS);
    client.property(ClientProperties.READ_TIMEOUT, REST_CLIENT_READ_TIMEOUT_MILIS);
    return client;
  }

  private class StreamRedirector implements Runnable {

    private final InputStream is_;
    private final OutputStream os_;

    public StreamRedirector(InputStream is, OutputStream os) {
      is_ = is;
      os_ = os;
    }

    @Override
    public void run() {
      int b;
      try {
        while ((b = is_.read()) != -1) {
          os_.write(b);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
