package com.datahub.plugins.common;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;


public class Validator {

  private Validator() {
  }

  public static void whiteSpacesValidation(String fieldName, String value) throws IllegalArgumentException {
    if (StringUtils.isEmpty(value) || StringUtils.containsWhitespace(value)) {
      throw new IllegalArgumentException(
          String.format("%s should not be empty and should not contains whitespaces", fieldName));
    }
  }

  public static void mapShouldNotBeEmpty(String fieldName, Map<String, Object> attributeMap)
      throws IllegalArgumentException {
    if (attributeMap.isEmpty()) {
      throw new IllegalArgumentException(String.format("%s should not be empty", fieldName));
    }
  }

  public static void listShouldNotBeEmpty(String fieldName, List<Object> list) throws IllegalArgumentException {
    if (list.isEmpty()) {
      throw new IllegalArgumentException(String.format("%s should not be empty", fieldName));
    }
  }

  public static void listShouldNotHaveDuplicate(String fieldName, List<String> list) {
    Set<String> set = new HashSet<>();
    list.forEach((input) -> {
      if (set.contains(input)) {
        throw new IllegalArgumentException(
            String.format("Duplicate entry of %s is found in %s. %s should not contain duplicate", input, fieldName,
                fieldName));
      }
      set.add(input);
    });
  }
}
