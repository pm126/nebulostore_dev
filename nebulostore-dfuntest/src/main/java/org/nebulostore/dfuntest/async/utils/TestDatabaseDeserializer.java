package org.nebulostore.dfuntest.async.utils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Deserializer of objects saved in database during asynchronous messages test.
 *
 * @author Piotr Malicki
 *
 */
public class TestDatabaseDeserializer implements Function<byte[], Map<CommAddress, Set<String>>> {

  @Override
  public Map<CommAddress, Set<String>> apply(byte[] input) {
    Type type = (new TypeToken<Map<String, Set<String>>>() { }).getType();
    Map<String, Set<String>> deserializedJson =  new Gson().fromJson(new String(input), type);
    Map<CommAddress, Set<String>> result = new HashMap<>();

    for (String peer : deserializedJson.keySet()) {
      result.put(new CommAddress(peer), deserializedJson.get(peer));
    }
    return result;
  }

}
