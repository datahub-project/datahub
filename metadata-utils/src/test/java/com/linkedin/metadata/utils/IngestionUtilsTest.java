package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class IngestionUtilsTest {

  private final String ingestionSourceUrn = "urn:li:ingestionSource:12345";

  @Test
  public void injectPipelineNameWhenThere() {
    String recipe =
        "{\"source\":{\"type\":\"snowflake\",\"config\":{\"stateful_ingestion\":{\"enabled\":true}}},\"pipeline_name\":\"test\"}";

    assertEquals(recipe, IngestionUtils.injectPipelineName(recipe, ingestionSourceUrn));
  }

  @Test
  public void resolveIngestionCliVersionUsesDefaultForNullBlankOrEmpty() {
    String def = "0.12.3";
    assertEquals(def, IngestionUtils.resolveIngestionCliVersion(null, def));
    assertEquals(def, IngestionUtils.resolveIngestionCliVersion("", def));
    assertEquals(def, IngestionUtils.resolveIngestionCliVersion("   ", def));
  }

  @Test
  public void resolveIngestionCliVersionUsesConfiguredWhenPresent() {
    assertEquals("1.2.3", IngestionUtils.resolveIngestionCliVersion("1.2.3", "0.12.3"));
    assertEquals("1.2.3", IngestionUtils.resolveIngestionCliVersion("  1.2.3  ", "0.12.3"));
  }

  @Test
  public void injectPipelineNameWhenNotThere() {
    String recipe =
        "{\"source\":{\"type\":\"snowflake\",\"config\":{\"stateful_ingestion\":{\"enabled\":true}}}}";
    recipe = IngestionUtils.injectPipelineName(recipe, ingestionSourceUrn);

    assertEquals(
        recipe,
        "{\"source\":{\"type\":\"snowflake\",\"config\":{\"stateful_ingestion\":{\"enabled\":true}}},\"pipeline_name\":\"urn:li:ingestionSource:12345\"}");
  }

  // CVE-2026-59171: org.json before 20260719 parsed arbitrarily long numbers via BigInteger /
  // BigDecimal in super-linear time. Newer releases keep numbers over 1,000 digits as strings.
  @Test
  public void injectPipelineNameWithHugeNumberIsFast() {
    String hugeNumber = "1".repeat(200_000);
    String recipe = "{\"source\":{\"type\":\"snowflake\",\"config\":{\"n\":" + hugeNumber + "}}}";

    long start = System.nanoTime();
    String result = IngestionUtils.injectPipelineName(recipe, ingestionSourceUrn);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertTrue(elapsedMs < 2_000, "injectPipelineName took " + elapsedMs + " ms");
    assertEquals(
        result,
        "{\"source\":{\"type\":\"snowflake\",\"config\":{\"n\":\""
            + hugeNumber
            + "\"}},\"pipeline_name\":\"urn:li:ingestionSource:12345\"}");
  }
}
