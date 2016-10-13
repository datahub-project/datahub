/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import wherehows.common.schemas.Record;


/**
 * Created by zechen on 10/21/15.
 */
public class StringUtil {

  public static String toDbString(Object object) {
    if (object != null) {
      return "'" + object.toString().replace("\\", "\\\\").replace("\'", "\\\'").replace("\"", "\\\"") + "'";
    } else {
      return "null";
    }
  }

  public static String toCsvString(Object object) {
    if (object != null) {
      return "\"" + object.toString().replace("\\", "\\\\").replace("\'", "\\\'").replace("\"", "\\\"") + "\"";
    } else {
      return "\\N";
    }
  }

  public static String replace(String s, String target, Object replacement) {
    if (replacement != null) {
      return s.replace(target, "'" + replacement.toString().replace("\'", "\\\'").replace("\"", "\\\"") + "'");
    } else {
      return s.replace(target, "null");
    }
  }

  /**
   * Convert objects of type Collection, Map, Array and AbstractRecord to Json string
   * @param obj
   * @return
   */
  public static Object objectToJsonString(Object obj) {
    if (obj instanceof Collection || obj instanceof Map || obj instanceof Object[] || obj instanceof Record) {
      try {
        return new ObjectMapper().writeValueAsString(obj);
      } catch (JsonProcessingException ex) {
        return obj;
      }
    }
    return obj;
  }

  public static Long toLong(Object obj) {
    return obj != null ? Long.valueOf(obj.toString()) : null;
  }

  public static Integer toInt(Object obj) {
    return obj != null ? Integer.valueOf(obj.toString()) : null;
  }

  public static Boolean toBoolean(Object obj) {
    return obj != null ? Boolean.valueOf(obj.toString()) : null;
  }

  /**
   * Convert Object with type Map<Object, Object> to Map<String, String>
   * @param obj Object with type Map<Object, Object>
   * @return Map <String, String>
   */
  public static Map<String, String> convertObjectMapToStringMap(Object obj) {
    final Map<Object, Object> map = (Map<Object, Object>) obj;
    final Map<String, String> metadata = new HashMap<>();
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      metadata.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }
    return metadata;
  }

  /**
   * Parse Long value from a String, if null or exception, return 0
   * @param text String
   * @return long
   */
  public static long parseLong(String text) {
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Parse Integer value from a String, if null or exception, return 0
   * @param text String
   * @return int
   */
  public static int parseInteger(String text) {
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Object to string, replace null/"null" with replacement string
   * @param obj Object
   * @return String
   */
  public static String toStringReplaceNull(Object obj, String replacement) {
    String string = String.valueOf(obj);
    return string == null || string.equals("null") ? replacement : string;
  }
}
