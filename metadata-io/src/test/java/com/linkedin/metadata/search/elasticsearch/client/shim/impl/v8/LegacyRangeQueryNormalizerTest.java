package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class LegacyRangeQueryNormalizerTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testNormalizeBetweenRange() throws Exception {
    String legacy =
        QueryBuilders.rangeQuery("age")
            .from(18)
            .to(65)
            .includeLower(true)
            .includeUpper(true)
            .toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains("\"from\""));
    assertFalse(normalized.contains("\"to\""));
    assertTrue(normalized.contains("\"gte\":18"));
    assertTrue(normalized.contains("\"lte\":65"));
  }

  @Test
  public void testNormalizeGteLtRange() throws Exception {
    String legacy = QueryBuilders.rangeQuery("timestamp").gte(100L).lt(200L).toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains("\"from\""));
    assertFalse(normalized.contains("\"to\""));
    assertTrue(normalized.contains("\"gte\":100"));
    assertTrue(normalized.contains("\"lt\":200"));
  }

  @Test
  public void testNormalizeGreaterThanRange() throws Exception {
    String legacy = QueryBuilders.rangeQuery("timestamp").gt(1731974400000L).toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains("\"from\""));
    assertTrue(normalized.contains("\"gt\":1731974400000"));
  }

  @Test
  public void testNormalizeNestedBoolRange() throws Exception {
    String legacy =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("timestamp").gte(1L).lt(2L))
            .toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains("\"from\""));
    assertFalse(normalized.contains("\"to\""));
    assertTrue(normalized.contains("\"gte\":1"));
    assertTrue(normalized.contains("\"lt\":2"));
  }

  @Test
  public void testNormalizeLessThanOrEqualRange() throws Exception {
    String legacy = QueryBuilders.rangeQuery("timestamp").lte(500L).toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains("\"to\""));
    assertTrue(normalized.contains("\"lte\":500"));
  }

  @Test
  public void testNormalizeAlreadyModernRangeUnchanged() throws Exception {
    String modern = "{\"range\":{\"timestamp\":{\"gte\":1,\"lt\":2,\"boost\":1.0}}}";

    String normalized = LegacyRangeQueryNormalizer.normalize(modern, objectMapper);

    assertEquals(objectMapper.readTree(modern), objectMapper.readTree(normalized));
  }

  @Test
  public void testNormalizeNullFromWithUpperBound() throws Exception {
    String legacy =
        QueryBuilders.rangeQuery("score").to(10).includeLower(true).includeUpper(true).toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains("\"from\""));
    assertTrue(normalized.contains("\"lte\":10"));
  }

  @Test
  public void testLeavesNonRangeQueryUnchanged() throws Exception {
    String legacy = QueryBuilders.termQuery("status", "active").toString();

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertEquals(objectMapper.readTree(legacy), objectMapper.readTree(normalized));
  }
}
