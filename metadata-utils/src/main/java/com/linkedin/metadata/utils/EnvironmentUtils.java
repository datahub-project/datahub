package com.linkedin.metadata.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for environment variable and system property management.
 *
 * <p>This class provides methods for:
 *
 * <ul>
 *   <li>Parsing command-line arguments in key=value format
 *   <li>Reading configuration values from environment variables and system properties
 *   <li>Type conversion for boolean, string, integer, and long values
 * </ul>
 *
 * <p>Environment variables take precedence over system properties for all configuration methods.
 */
public class EnvironmentUtils {

  private static final String KEY_VALUE_DELIMITER = "=";

  /**
   * Gets a string value from environment variables or system properties. Environment variables take
   * precedence over system properties.
   *
   * @param key the configuration key
   * @return the string value or null if not set
   */
  private static String getValue(String key) {
    // Check environment variables first, then system properties for testing
    String value = System.getenv(key);
    if (value == null) {
      value = System.getProperty(key);
    }
    return value;
  }

  /**
   * Parses a list of command-line arguments in key=value format.
   *
   * @param args the list of arguments to parse
   * @return a map where keys are argument names and values are Optional containing the value or
   *     empty if no value provided
   */
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
    String value = getValue(key);
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
    String value = getValue(key);
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
    String value = getValue(key);
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
    String value = getValue(key);
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
    return getValue(key) != null;
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

  private EnvironmentUtils() {}
}
