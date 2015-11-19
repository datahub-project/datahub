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
package utils;

import com.fasterxml.jackson.databind.JsonNode;


/**
 * Created by zechen on 10/16/15.
 */
public class JsonUtil {

  public static Object getJsonValue(JsonNode input, String name, Class type) throws IllegalArgumentException {
    return getJsonValue(input, name, type, true, null);
  }

  public static Object getJsonValue(JsonNode input, String name, Class type, Object defaultValue) throws IllegalArgumentException {
    return getJsonValue(input, name, type, false, defaultValue);
  }

  public static Object getJsonValue(JsonNode input, String name, Class type, boolean isRequired, Object defaultValue) throws IllegalArgumentException {
    JsonNode node = input.findPath(name);

    if (node.isMissingNode() || node.isNull()) {
      if (isRequired) {
        throw new IllegalArgumentException(name + " is required!");
      } else {
        return defaultValue;
      }
    }

    if (type.equals(String.class)) {
      return node.textValue();
    }

    if (type.equals(Integer.class)) {
      return node.asInt();
    }

    if (type.equals(Long.class)) {
      return node.asLong();
    }

    if (type.equals(Boolean.class)) {
      return node.asBoolean();
    }

    if (type.equals(Double.class)) {
      return node.asDouble();
    }

    return node.asText();
  }
}
