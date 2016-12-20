package org.nebulostore.dfuntest.async.utils;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.gson.Gson;

import org.nebulostore.communication.naming.CommAddress;
/**
 * Serializer of messages info objects saved in database during asynchronous messages test.
 *
 * @author Piotr Malicki
 *
 */
public class MessagesInfoSerializer implements Function<Map<CommAddress, Set<String>>, String> {

  @Override
  public String apply(Map<CommAddress, Set<String>> input) {
    return new Gson().toJson(input);
  }

}
