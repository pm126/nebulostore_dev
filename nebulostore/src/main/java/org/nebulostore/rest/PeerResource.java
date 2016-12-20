package org.nebulostore.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.apache.log4j.Logger;

@Path("peer/")
public class PeerResource {

  @GET
  @Path("ping")
  public boolean ping() {
    Logger.getLogger(PeerResource.class).debug("Ping request received");
    return true;
  }
}
