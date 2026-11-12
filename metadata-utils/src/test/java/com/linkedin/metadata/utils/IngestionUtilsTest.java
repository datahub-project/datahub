package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class IngestionUtilsTest {

  private final String ingestionSourceUrn = "urn:li:ingestionSource:12345";
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void extractSourceTypeFromRecipe() {
    assertEquals(
        IngestionUtils.extractSourceType(
            mapper, "{\"source\":{\"type\":\"snowflake\",\"config\":{\"account_id\":\"abc\"}}}"),
        "snowflake");
    // source present but no type, malformed JSON, empty, and null all return null
    assertNull(IngestionUtils.extractSourceType(mapper, "{\"source\":{\"config\":{}}}"));
    assertNull(IngestionUtils.extractSourceType(mapper, "{not valid json"));
    assertNull(IngestionUtils.extractSourceType(mapper, ""));
    assertNull(IngestionUtils.extractSourceType(mapper, null));
  }

  @Test
  public void injectPipelineNameWhenThere() {
    String recipe =
        "{\"source\":{\"type\":\"snowflake\",\"config\":{\"stateful_ingestion\":{\"enabled\":true}}},\"pipeline_name\":\"test\"}";

    assertEquals(recipe, IngestionUtils.injectPipelineName(recipe, ingestionSourceUrn));
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
