package org.nebulostore.rest;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
public class RestModuleImpl implements RestModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestModuleImpl.class);
  private final AtomicBoolean isTerminate_;
  private final String host_;
  private final int port_;
  private final Injector injector_;

  @Inject
  public RestModuleImpl(@Named("rest-api.server-config.host") String host,
                        @Named("rest-api.server-config.port") int port,
                        Injector injector) {
    host_ = host;
    port_ = port;
    injector_ = injector;
    isTerminate_ = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    LOGGER.debug("Running rest module");
    URI uri = URI.create(String.format("%s:%s/", host_, port_));
    LOGGER.debug("URI created");
    ResourceConfig config = configureServer();
    LOGGER.debug("Server configured");
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, config);
    LOGGER.debug("Http server created");

    try {
      LOGGER.debug("Starting http server...");
      httpServer.start();
      LOGGER.debug("Started http server");
      waitForTermination();
      httpServer.shutdown();
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage());
    }

  }

  private void waitForTermination() throws InterruptedException {
    synchronized (isTerminate_) {
      while (!isTerminate_.get()) {
        isTerminate_.wait();
      }
    }
  }

  @Override
  public void shutDown() {
    synchronized (isTerminate_) {
      isTerminate_.set(true);
      isTerminate_.notify();
    }
  }

  private ResourceConfig configureServer() {
    LOGGER.debug("Creating resource config");
    ResourceConfig resourceConfig = new RestResourceConfig(MultiPartFeature.class);
    LOGGER.debug("Adding package org.nebulostore");
    resourceConfig.packages(true, "org.nebulostore");
    LOGGER.debug("Packages added");
    return resourceConfig;
  }

  private class RestResourceConfig extends ResourceConfig {

    public RestResourceConfig(Class<?>... classes) {
      super(classes);

      register(new ContainerLifecycleListener() {

        @Override
        public void onStartup(Container container) {
          LOGGER.debug("Starting config");
          ServiceLocator serviceLocator = container.getApplicationHandler().getServiceLocator();
          LOGGER.debug("Got service locator");

          GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
          LOGGER.debug("Initialized guice bridge");
          GuiceIntoHK2Bridge guiceBridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);
          LOGGER.debug("Got guice bridge service");
          guiceBridge.bridgeGuiceInjector(injector_);
          LOGGER.debug("Bridged injector");
        }

        @Override
        public void onShutdown(Container container) {
        }

        @Override
        public void onReload(Container container) {
        }
      });
    }
  }

}
