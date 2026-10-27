package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertEquals;

import com.datahub.context.OperationFingerprint;
import org.testng.annotations.Test;

public class ConfiguredIndexPrefixResolverTest {

  @Test
  public void testReturnsConfiguredPrefix() {
    assertEquals(
        new ConfiguredIndexPrefixResolver("acme").resolvePrefix(OperationFingerprint.EMPTY),
        "acme");
  }

  @Test
  public void testNullPrefixResolvesToEmpty() {
    assertEquals(
        new ConfiguredIndexPrefixResolver(null).resolvePrefix(OperationFingerprint.EMPTY), "");
  }

  @Test
  public void testEmptyPrefixResolvesToEmpty() {
    assertEquals(
        new ConfiguredIndexPrefixResolver("").resolvePrefix(OperationFingerprint.EMPTY), "");
  }
}
