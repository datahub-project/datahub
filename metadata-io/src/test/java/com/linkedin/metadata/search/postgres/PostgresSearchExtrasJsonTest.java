package com.linkedin.metadata.search.postgres;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class PostgresSearchExtrasJsonTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void buildSearchExtrasPayloadStripsTierKeys() throws Exception {
    String doc =
        "{\"_search\":{\"tier_1\":\"a\",\"tier_2\":\"b\",\"entityName\":\"n\",\"qualifiedName\":\"q\"}}";
    String out = PostgresSearchExtrasJson.buildSearchExtrasPayload(doc);
    JsonNode n = MAPPER.readTree(out);
    assertTrue(n.has("entityName"));
    assertTrue(n.has("qualifiedName"));
    assertFalse(n.has("tier_1"));
    assertFalse(n.has("tier_2"));
  }

  @Test
  public void buildSearchExtrasPayloadReturnsNullWhenOnlyTiers() {
    String doc = "{\"_search\":{\"tier_1\":\"only\"}}";
    assertNull(PostgresSearchExtrasJson.buildSearchExtrasPayload(doc));
  }

  @Test
  public void buildSearchExtrasPayloadReturnsNullWithoutSearch() {
    assertNull(PostgresSearchExtrasJson.buildSearchExtrasPayload("{\"urn\":\"x\"}"));
  }
}
