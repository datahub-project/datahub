package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

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
}
