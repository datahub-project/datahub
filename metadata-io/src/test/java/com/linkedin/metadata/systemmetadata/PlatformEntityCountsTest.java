package com.linkedin.metadata.systemmetadata;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class PlatformEntityCountsTest {

  @Test
  public void testNormalizePlatform_shortId() {
    assertEquals(PlatformEntityCounts.normalizePlatform("snowflake"), "snowflake");
  }

  @Test
  public void testNormalizePlatform_urn() {
    assertEquals(
        PlatformEntityCounts.normalizePlatform("urn:li:dataPlatform:snowflake"), "snowflake");
  }

  @Test
  public void testNormalizePlatform_missing() {
    assertEquals(
        PlatformEntityCounts.normalizePlatform(PlatformEntityCounts.NO_PLATFORM), "NO_PLATFORM");
    assertEquals(PlatformEntityCounts.normalizePlatform(""), "NO_PLATFORM");
    assertEquals(PlatformEntityCounts.normalizePlatform("   "), "NO_PLATFORM");
  }
}
