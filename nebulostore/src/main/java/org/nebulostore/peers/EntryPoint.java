package org.nebulostore.peers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * NebuloStore entry point.
 * Reads peer class name from configuration file, creates and runs it.
 * @author Bolek Kulbabinski
 */
public final class EntryPoint {
  private static Logger logger_ = Logger.getLogger(EntryPoint.class);
  private static final String CONFIGURATION_PATH = "resources/conf/Peer.xml";
  private static final String DEFAULT_PEER_CLASS = "org.nebulostore.appcore.Peer";
  private static final int GROUPS_NUMBER = 5;
  private static final int OUT_SYMBOLS_NUMBER = 18;
  private static final int IN_SYMBOLS_NUMBER = 5;
  private static final int GLOBAL_REDUNDANCY_SIZE = 10;
  private static final int FINAL_SIZE = OUT_SYMBOLS_NUMBER + (GROUPS_NUMBER - 1) *
      (OUT_SYMBOLS_NUMBER - IN_SYMBOLS_NUMBER - GLOBAL_REDUNDANCY_SIZE);
  private static final int DATA_LENGTH = 1024 * 1024;

  public static void main(String[] args) throws NebuloException {
    try {
      logger_.info("Starting nebulostore");
      XMLConfiguration config = initConfig();
      logger_.info("Init config prepared");
      setDefaultThreadUncaughtExceptionHandler();
      logger_.info("Uncaught exceeption handler set");
      AbstractPeer peer = createPeer(config);
      logger_.info("Peer created");
      Thread peerThread = new Thread(peer, "Peer Main Thread");
      logger_.info("Peer thread created");
      peerThread.start();
      logger_.info("Peer thread started");
      peerThread.join();
    } catch (NebuloException exception) {
      logger_.fatal("Unable to start NebuloStore! (" + exception.getMessage() + ")", exception);
    } catch (InterruptedException e) {
      logger_.fatal("InterruptedException while waiting for peer thread!", e);
    }
  }

  /**
   * Default exception handler for threads.
   *
   * Logs error down and shuts down the application.
   * @author Grzegorz Milka
   */
  private static final class NebuloUncaughtExceptionHandler implements
    Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      logger_.fatal("Thread: " + t + " has caught an irrecoverable " +
          "exception: " + e + ". Shutting down Nebulostore.", e);
      System.exit(1);
    }
  }

  private static void setDefaultThreadUncaughtExceptionHandler() {
    try {
      Thread.setDefaultUncaughtExceptionHandler(
          new NebuloUncaughtExceptionHandler());
    } catch (SecurityException e) {
      logger_.warn("Caught security exception when setting exception handler.", e);
    }
  }

  private static XMLConfiguration initConfig() throws NebuloException {
    try {
      return new XMLConfiguration(CONFIGURATION_PATH);
    } catch (ConfigurationException cex) {
      throw new NebuloException("Configuration read error in: " + CONFIGURATION_PATH, cex);
    }
  }

  private static AbstractPeer createPeer(XMLConfiguration xmlConfig) throws NebuloException {
    long startTime = System.currentTimeMillis();
    System.out.println("Starttime: " + startTime);
    logger_.info("Creating peer");
    String className = xmlConfig.getString("class-name", DEFAULT_PEER_CLASS);
    logger_.info("Class name: " + className);
    String confClassName = xmlConfig.getString("configuration-class-name",
        className + "Configuration");
    logger_.info("Conf class name: " + confClassName);

    System.out.println("Conf class name: " + confClassName);
    Class<?> configurationClass = loadConfigurationClass(confClassName, className);
    logger_.info("Configuration class loaded: " + configurationClass);

    try {
      GenericConfiguration genericConfig = null;

      if (configurationClass == null) {
        logger_.warn("Configuration class not found, using default.");
        Class<? extends AbstractPeer> peerClass =
            (Class<? extends AbstractPeer>) Class.forName(className);
        genericConfig = new DefaultConfiguration(peerClass);
      } else {
        logger_.info("Preparing conf class constructor");
        Constructor<?> ctor = configurationClass.getConstructor();
        logger_.info("Got constructor");
        genericConfig = (GenericConfiguration) ctor.newInstance();
        logger_.info("Got configuration instance");
      }
      genericConfig.setXMLConfig(xmlConfig);
      logger_.info("Set XML config");

      Injector injector = Guice.createInjector(genericConfig);
      logger_.info("Guice injector created");
      System.out.println("Injector time: " + (System.currentTimeMillis() - startTime));
      AbstractPeer peer = injector.getInstance(AbstractPeer.class);
      logger_.info("Created peer, returning.");
      return peer;
    } catch (InstantiationException e) {
      throw new NebuloException("Could not instantiate class " + className + ".", e);
    } catch (IllegalAccessException e) {
      throw new NebuloException("Constructor for class " + className + " is not accessible.", e);
    } catch (ClassNotFoundException e) {
      throw new NebuloException("Class " + className + " not found.", e);
    } catch (SecurityException e) {
      throw new NebuloException("Could not access constructor of class " + className +
          " due to SecurityException.", e);
    } catch (NoSuchMethodException e) {
      throw new NebuloException("Constructor for class " + className + " is not accessible.", e);
    } catch (IllegalArgumentException e) {
      throw new NebuloException("Incorrect parameters for constructor for " + className + ".", e);
    } catch (InvocationTargetException e) {
      throw new NebuloException("Unable to invoke constructor for " + className + ".", e);
    }
  }

  private static Class<?> loadConfigurationClass(String confClassName, String className) {
    try {
      return Class.forName(confClassName);
    } catch (ClassNotFoundException e1) {
      return null;
    }
  }

  /**
   * Default configuration using given peer name.
   * @author Bolek Kulbabinski
   */
  private static class DefaultConfiguration extends PeerConfiguration {
    Class<? extends AbstractPeer> peerClass_;

    protected DefaultConfiguration(Class<? extends AbstractPeer> peerClass) {
      peerClass_ = peerClass;
    }

    @Override
    protected void configurePeer() {
      bind(AbstractPeer.class).to(peerClass_).in(Singleton.class);
    }
  }

  private EntryPoint() { }
}
