package com.linkedin.metadata.utils;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class BasePathUtilsTest {

  @Test
  public void testResolveBasePathWithBoolean() {
    // Test case 1: basePathEnabled = false, should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(false, "/test-path"));

    // Test case 2: basePathEnabled = true, basePath = null, should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(true, null));

    // Test case 3: basePathEnabled = true, basePath = "", should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(true, ""));

    // Test case 4: basePathEnabled = true, basePath = "/", should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(true, "/"));

    // Test case 5: basePathEnabled = true, basePath = "/test-path", should return "/test-path"
    assertEquals("/test-path", BasePathUtils.resolveBasePath(true, "/test-path"));

    // Test case 6: basePathEnabled = true, basePath = "test-path", should return "test-path"
    assertEquals("test-path", BasePathUtils.resolveBasePath(true, "test-path"));
  }

  @Test
  public void testResolveBasePathWithBooleanObject() {
    // Test case 1: basePathEnabled = null, should return empty string
    assertEquals("", BasePathUtils.resolveBasePath((Boolean) null, "/test-path"));

    // Test case 2: basePathEnabled = false, should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(Boolean.FALSE, "/test-path"));

    // Test case 3: basePathEnabled = true, basePath = null, should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(Boolean.TRUE, null));

    // Test case 4: basePathEnabled = true, basePath = "/", should return empty string
    assertEquals("", BasePathUtils.resolveBasePath(Boolean.TRUE, "/"));

    // Test case 5: basePathEnabled = true, basePath = "/test-path", should return "/test-path"
    assertEquals("/test-path", BasePathUtils.resolveBasePath(Boolean.TRUE, "/test-path"));
  }

  @Test
  public void testResolveBasePathWithDefaultValue() {
    // Test case 1: basePathEnabled = false, should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(false, "/test-path", "/"));

    // Test case 2: basePathEnabled = true, basePath = null, should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(true, null, "/"));

    // Test case 3: basePathEnabled = true, basePath = "", should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(true, "", "/"));

    // Test case 4: basePathEnabled = true, basePath = "/", should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(true, "/", "/"));

    // Test case 5: basePathEnabled = true, basePath = "/test-path", should return "/test-path"
    assertEquals("/test-path", BasePathUtils.resolveBasePath(true, "/test-path", "/"));

    // Test case 6: Custom default value
    assertEquals(
        "custom-default", BasePathUtils.resolveBasePath(false, "/test-path", "custom-default"));
  }

  @Test
  public void testResolveBasePathWithBooleanObjectAndDefaultValue() {
    // Test case 1: basePathEnabled = null, should return default value
    assertEquals("/", BasePathUtils.resolveBasePath((Boolean) null, "/test-path", "/"));

    // Test case 2: basePathEnabled = false, should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(Boolean.FALSE, "/test-path", "/"));

    // Test case 3: basePathEnabled = true, basePath = null, should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(Boolean.TRUE, null, "/"));

    // Test case 4: basePathEnabled = true, basePath = "/", should return default value
    assertEquals("/", BasePathUtils.resolveBasePath(Boolean.TRUE, "/", "/"));

    // Test case 5: basePathEnabled = true, basePath = "/test-path", should return "/test-path"
    assertEquals("/test-path", BasePathUtils.resolveBasePath(Boolean.TRUE, "/test-path", "/"));

    // Test case 6: Custom default value
    assertEquals(
        "custom-default",
        BasePathUtils.resolveBasePath(Boolean.FALSE, "/test-path", "custom-default"));
  }

  @Test
  public void testNormalizeBasePath() {
    // Test case 1: null input
    assertEquals("", BasePathUtils.normalizeBasePath(null));

    // Test case 2: empty string
    assertEquals("", BasePathUtils.normalizeBasePath(""));

    // Test case 3: already normalized
    assertEquals("/test-path", BasePathUtils.normalizeBasePath("/test-path"));

    // Test case 4: trailing slash
    assertEquals("/test-path", BasePathUtils.normalizeBasePath("/test-path/"));

    // Test case 5: no leading slash
    assertEquals("/test-path", BasePathUtils.normalizeBasePath("test-path"));

    // Test case 6: no leading slash with trailing slash
    assertEquals("/test-path", BasePathUtils.normalizeBasePath("test-path/"));

    // Test case 7: just a slash
    assertEquals("", BasePathUtils.normalizeBasePath("/"));

    // Test case 8: multiple slashes
    assertEquals("/test/path", BasePathUtils.normalizeBasePath("/test/path/"));
  }

  @Test
  public void testAddBasePath() {
    // Test case 1: null base path
    assertEquals("/login", BasePathUtils.addBasePath("/login", null));

    // Test case 2: empty base path
    assertEquals("/login", BasePathUtils.addBasePath("/login", ""));

    // Test case 3: normal case
    assertEquals("/datahub/login", BasePathUtils.addBasePath("/login", "/datahub"));

    // Test case 4: base path with trailing slash
    assertEquals("/datahub/login", BasePathUtils.addBasePath("/login", "/datahub/"));

    // Test case 5: path without leading slash
    assertEquals("/datahub/login", BasePathUtils.addBasePath("login", "/datahub"));

    // Test case 6: both without leading slashes
    assertEquals("/datahub/login", BasePathUtils.addBasePath("login", "datahub"));

    // Test case 7: root path
    assertEquals("/datahub/", BasePathUtils.addBasePath("/", "/datahub"));
  }

  @Test
  public void testStripBasePath() {
    // Test case 1: null base path
    assertEquals("/login", BasePathUtils.stripBasePath("/login", null));

    // Test case 2: empty base path
    assertEquals("/login", BasePathUtils.stripBasePath("/login", ""));

    // Test case 3: normal case
    assertEquals("/login", BasePathUtils.stripBasePath("/datahub/login", "/datahub"));

    // Test case 4: base path with trailing slash
    assertEquals("/login", BasePathUtils.stripBasePath("/datahub/login", "/datahub/"));

    // Test case 5: path doesn't start with base path
    assertEquals("/other/login", BasePathUtils.stripBasePath("/other/login", "/datahub"));

    // Test case 6: exact match
    assertEquals("/", BasePathUtils.stripBasePath("/datahub", "/datahub"));

    // Test case 7: root path
    assertEquals("/", BasePathUtils.stripBasePath("/", "/datahub"));

    // Test case 8: multiple levels
    assertEquals("/api/v1/login", BasePathUtils.stripBasePath("/datahub/api/v1/login", "/datahub"));
  }
}
