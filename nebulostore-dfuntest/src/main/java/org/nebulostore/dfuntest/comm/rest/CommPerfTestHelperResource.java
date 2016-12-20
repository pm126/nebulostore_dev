package org.nebulostore.dfuntest.comm.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;

import org.nebulostore.dfuntest.comm.CommPerfTestHelperModule;

@Path("comm_perf_helper/")
public class CommPerfTestHelperResource {

  private final CommPerfTestHelperModule helper_;

  @Inject
  public CommPerfTestHelperResource(CommPerfTestHelperModule helper) {
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
  @Path("stop_sending")
  public Response stopMessagesSending() {
    helper_.stopMessagesSending();
    return Response.ok().build();
  }
}
