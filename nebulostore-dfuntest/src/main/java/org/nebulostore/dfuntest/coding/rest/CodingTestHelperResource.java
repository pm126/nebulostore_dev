package org.nebulostore.dfuntest.coding.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.GsonBuilder;

import org.apache.log4j.Logger;
import org.nebulostore.dfuntest.coding.CodingTestHelperModule;

/**
 *
 * @author Piotr Malicki
 *
 */

@Path("coding_helper/")
public class CodingTestHelperResource {

  private static Logger logger_ = Logger.getLogger(CodingTestHelperResource.class);

  private final CodingTestHelperModule helper_;

  @Inject
  public CodingTestHelperResource(CodingTestHelperModule helper) {
    helper_ = helper;
  }

  @HEAD
  @Path("store_objects")
  public Response storeObjects() {
    helper_.storeObjects();
    return Response.ok().build();
  }

  @HEAD
  @Path("start_reader_mode")
  public Response startInReaderMode() {
    logger_.debug("Starting reader mode");
    helper_.startReaderMode();
    logger_.debug("Started reader mode");
    return Response.ok().build();
  }

  @HEAD
  @Path("stop_reader_mode")
  public Response stopReaderMode() {
    logger_.debug("Stopping reader mode");
    helper_.stopReaderMode();
    logger_.debug("Stopped reader mode");
    return Response.ok().build();
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("test_results")
  public String getTestResults() {
    logger_.debug("Returning test results");
    return new GsonBuilder().enableComplexMapKeySerialization().create().
        toJson(helper_.getTestResults());
  }

}
