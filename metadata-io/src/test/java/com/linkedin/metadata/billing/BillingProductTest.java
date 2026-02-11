package com.linkedin.metadata.billing;

import static org.testng.Assert.*;

import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;

/** Unit tests for BillingProduct. */
public class BillingProductTest {

  @Test
  public void testAskDataHubConfigKey() {
    assertEquals(
        BillingProduct.ASK_DATAHUB.getConfigKey(),
        "askDataHubProductId",
        "ASK_DATAHUB should map to askDataHubProductId config key");
  }

  @Test
  public void testConfigKeysAreNotEmpty() {
    for (BillingProduct product : BillingProduct.values()) {
      assertNotNull(product.getConfigKey(), product.name() + " config key must not be null");
      assertFalse(
          product.getConfigKey().trim().isEmpty(),
          product.name() + " config key must not be empty");
    }
  }

  @Test
  public void testConfigKeysAreUnique() {
    Set<String> seen = new HashSet<>();
    for (BillingProduct product : BillingProduct.values()) {
      assertTrue(
          seen.add(product.getConfigKey()),
          "Duplicate config key found: " + product.getConfigKey() + " on " + product.name());
    }
  }
}
