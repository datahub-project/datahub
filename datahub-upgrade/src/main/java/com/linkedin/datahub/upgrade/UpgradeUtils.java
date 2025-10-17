package com.linkedin.datahub.upgrade;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UpgradeUtils {

  private static final String KEY_VALUE_DELIMITER = "=";

  public static Map<String, Optional<String>> parseArgs(final List<String> args) {
    if (args == null) {
      return Collections.emptyMap();
    }
    final Map<String, Optional<String>> parsedArgs = new HashMap<>();

    for (final String arg : args) {
      List<String> parsedArg = Arrays.asList(arg.split(KEY_VALUE_DELIMITER, 2));
      parsedArgs.put(
          parsedArg.get(0),
          parsedArg.size() > 1 ? Optional.of(parsedArg.get(1)) : Optional.empty());
    }
    return parsedArgs;
  }

  /**
   * Gets a boolean value from environment variables or system properties. Environment variables
   * take precedence over system properties.
   *
   * @param key the configuration key
   * @param defaultValue the default value if neither env var nor system property is set
   * @return the boolean value
   */
  public static boolean getBoolean(String key, boolean defaultValue) {
    // Check environment variables first, then system properties for testing
    String value = System.getenv(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    if (value == null) {
      return defaultValue;
    }
    return "true".equalsIgnoreCase(value) || "1".equals(value);
  }

  /**
   * Gets a string value from environment variables or system properties. Environment variables take
   * precedence over system properties.
   *
   * @param key the configuration key
   * @param defaultValue the default value if neither env var nor system property is set
   * @return the string value
   */
  public static String getString(String key, String defaultValue) {
    // Check environment variables first, then system properties for testing
    String value = System.getenv(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    return value != null ? value : defaultValue;
  }

  /**
   * Gets an integer value from environment variables or system properties. Environment variables
   * take precedence over system properties.
   *
   * @param key the configuration key
   * @param defaultValue the default value if neither env var nor system property is set
   * @return the integer value
   */
  public static int getInt(String key, int defaultValue) {
    // Check environment variables first, then system properties for testing
    String value = System.getenv(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Gets a long value from environment variables or system properties. Environment variables take
   * precedence over system properties.
   *
   * @param key the configuration key
   * @param defaultValue the default value if neither env var nor system property is set
   * @return the long value
   */
  public static long getLong(String key, long defaultValue) {
    // Check environment variables first, then system properties for testing
    String value = System.getenv(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Checks if a configuration key is set (either as environment variable or system property).
   *
   * @param key the configuration key
   * @return true if the key is set, false otherwise
   */
  public static boolean isSet(String key) {
    return System.getenv(key) != null || System.getProperty(key) != null;
  }

  /**
   * Gets a string value from environment variables or system properties, returning null if not set.
   * Environment variables take precedence over system properties.
   *
   * @param key the configuration key
   * @return the string value or null if not set
   */
  public static String getString(String key) {
    return getString(key, null);
  }

  private UpgradeUtils() {}
}
