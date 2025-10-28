package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EnvironmentUtilsTest {

  @BeforeMethod
  public void setUp() {
    // Clear all system properties that might interfere between tests
    clearSystemProperties();
  }

  @AfterMethod
  public void tearDown() {
    // Clean up system properties after each test
    clearSystemProperties();
  }

  private void clearSystemProperties() {
    // Clear all system properties used in tests
    System.clearProperty("TEST_BOOL_TRUE");
    System.clearProperty("TEST_BOOL_ONE");
    System.clearProperty("TEST_BOOL_FALSE");
    System.clearProperty("TEST_BOOL_MISSING");
    System.clearProperty("TEST_STRING_ENV");
    System.clearProperty("TEST_INT_ENV");
    System.clearProperty("TEST_INT_ENV_INVALID");
  }

  @Test
  public void testParseArgs() {
    // Test with valid key=value pairs
    List<String> args = Arrays.asList("key1=value1", "key2=value2", "key3=");
    Map<String, Optional<String>> result = EnvironmentUtils.parseArgs(args);

    assertEquals(result.size(), 3);
    assertTrue(result.get("key1").isPresent());
    assertEquals(result.get("key1").get(), "value1");
    assertTrue(result.get("key2").isPresent());
    assertEquals(result.get("key2").get(), "value2");
    assertTrue(result.get("key3").isPresent());
    assertEquals(result.get("key3").get(), "");

    // Test with null input
    Map<String, Optional<String>> nullResult = EnvironmentUtils.parseArgs(null);
    assertTrue(nullResult.isEmpty());

    // Test with empty list
    Map<String, Optional<String>> emptyResult = EnvironmentUtils.parseArgs(Collections.emptyList());
    assertTrue(emptyResult.isEmpty());
  }

  @Test
  public void testGetBooleanEnv() {
    // Test with "true" value
    System.setProperty("TEST_BOOL_TRUE", "true");
    boolean resultTrue = EnvironmentUtils.getBoolean("TEST_BOOL_TRUE", false);
    assertTrue(resultTrue);

    // Test with "1" value
    System.setProperty("TEST_BOOL_ONE", "1");
    boolean resultOne = EnvironmentUtils.getBoolean("TEST_BOOL_ONE", false);
    assertTrue(resultOne);

    // Test with "false" value
    System.setProperty("TEST_BOOL_FALSE", "false");
    boolean resultFalse = EnvironmentUtils.getBoolean("TEST_BOOL_FALSE", true);
    assertFalse(resultFalse);

    // Test with missing environment variable
    System.clearProperty("TEST_BOOL_MISSING");
    boolean resultMissing = EnvironmentUtils.getBoolean("TEST_BOOL_MISSING", true);
    assertTrue(resultMissing);
  }

  @Test
  public void testGetStringEnv() {
    System.setProperty("TEST_STRING_ENV", "test_value");
    String result = EnvironmentUtils.getString("TEST_STRING_ENV", "default_value");
    assertEquals(result, "test_value");

    System.clearProperty("TEST_STRING_ENV");
    String defaultResult = EnvironmentUtils.getString("TEST_STRING_ENV", "default_value");
    assertEquals(defaultResult, "default_value");
  }

  @Test
  public void testGetIntEnv() {
    System.setProperty("TEST_INT_ENV", "42");
    int result = EnvironmentUtils.getInt("TEST_INT_ENV", 10);
    assertEquals(result, 42);

    System.clearProperty("TEST_INT_ENV");
    int defaultResult = EnvironmentUtils.getInt("TEST_INT_ENV", 10);
    assertEquals(defaultResult, 10);

    System.setProperty("TEST_INT_ENV_INVALID", "not_a_number");
    int invalidResult = EnvironmentUtils.getInt("TEST_INT_ENV_INVALID", 10);
    assertEquals(invalidResult, 10);
  }

  @Test
  public void testGetLongEnv() {
    System.setProperty("TEST_LONG_ENV", "123456789");
    long result = EnvironmentUtils.getLong("TEST_LONG_ENV", 0L);
    assertEquals(result, 123456789L);

    System.clearProperty("TEST_LONG_ENV");
    long defaultResult = EnvironmentUtils.getLong("TEST_LONG_ENV", 0L);
    assertEquals(defaultResult, 0L);

    System.setProperty("TEST_LONG_ENV_INVALID", "not_a_number");
    long invalidResult = EnvironmentUtils.getLong("TEST_LONG_ENV_INVALID", 0L);
    assertEquals(invalidResult, 0L);
  }

  @Test
  public void testIsSet() {
    // Test with set property
    System.setProperty("TEST_IS_SET", "value");
    assertTrue(EnvironmentUtils.isSet("TEST_IS_SET"));

    // Test with unset property
    System.clearProperty("TEST_IS_NOT_SET");
    assertFalse(EnvironmentUtils.isSet("TEST_IS_NOT_SET"));
  }

  @Test
  public void testGetStringWithoutDefault() {
    // Test with set property
    System.setProperty("TEST_STRING_NO_DEFAULT", "test_value");
    String result = EnvironmentUtils.getString("TEST_STRING_NO_DEFAULT");
    assertEquals(result, "test_value");

    // Test with unset property
    System.clearProperty("TEST_STRING_NO_DEFAULT");
    String nullResult = EnvironmentUtils.getString("TEST_STRING_NO_DEFAULT");
    assertEquals(nullResult, null);
  }
}
