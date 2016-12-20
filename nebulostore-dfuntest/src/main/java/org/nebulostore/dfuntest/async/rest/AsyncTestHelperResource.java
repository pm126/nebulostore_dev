package org.nebulostore.dfuntest.async.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;

import org.nebulostore.dfuntest.async.AsyncTestHelperModule;

/**
 * Rest resource for asynchronous messages test helper module.
 * @author Piotr Malicki
 *
 */
@Path("async_helper/")
public class AsyncTestHelperResource {

  private final AsyncTestHelperModule helper_;

  @Inject
  public AsyncTestHelperResource(AsyncTestHelperModule helper) {
    helper_ = helper;
  }

  @GET
  @Path("received_messages")
  @Produces(MediaType.APPLICATION_JSON)
  public String getReceivedMessages() {
    return new Gson().toJson(helper_.getReceivedMessagesIds());
  }

  @GET
  @Path("sent_messages")
  @Produces(MediaType.APPLICATION_JSON)
  public String getSentMessages() {
    return new Gson().toJson(helper_.getSentMessagesIds());
  }

  @HEAD
  @Path("start_sending")
  public Response startMessagesSending() {
    helper_.startMessagesSending();
    return Response.ok().build();
  }

  @HEAD
  @Path("stop_sending")
  public Response stopMessagesSending() {
    helper_.stopMessagesSending();
    return Response.ok().build();
  }

  @GET
  @Path("messages_delays")
  @Produces(MediaType.APPLICATION_JSON)
  public String getMessagesDelays() {
    return new Gson().toJson(helper_.getMessagesDelays());
  }

}
