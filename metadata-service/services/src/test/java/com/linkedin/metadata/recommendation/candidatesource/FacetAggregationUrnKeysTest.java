package com.linkedin.metadata.recommendation.candidatesource;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class FacetAggregationUrnKeysTest {

  @Test
  public void coerceLeavesBareUrnUnchanged() {
    assertEquals(
        FacetAggregationUrnKeys.coerceFacetKeyToUrnString("  urn:li:tag:Legacy  "),
        "urn:li:tag:Legacy");
  }

  @Test
  public void coerceExtractsJsonArrayEncodedSingleUrn() {
    assertEquals(
        FacetAggregationUrnKeys.coerceFacetKeyToUrnString("[\"urn:li:tag:Legacy\"]"),
        "urn:li:tag:Legacy");
  }

  @Test
  public void coerceHandlesWhitespaceInsideArray() {
    assertEquals(
        FacetAggregationUrnKeys.coerceFacetKeyToUrnString("[  \"urn:li:tag:Legacy\"  ]"),
        "urn:li:tag:Legacy");
  }
}
