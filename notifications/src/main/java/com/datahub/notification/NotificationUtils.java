package com.datahub.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;


/**
 * Helper methods used for notification sinks.
 */
public class NotificationUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Deserializes a json string into a list of strings.
   */
  public static List<String> jsonToStrList(final String jsonList) {
    try {
      String[] listArray = OBJECT_MAPPER.readValue(jsonList, String[].class);
      return Arrays.asList(listArray);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(String.format("Failed to convert provided string to json list %s", jsonList), e);
    }
  }

  private NotificationUtils() { }

}
