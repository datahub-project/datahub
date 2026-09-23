package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringReader;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class LegacyRangeQueryNormalizerTest {

  private static final String ADJUST_PURE_NEGATIVE = "adjust_pure_negative";

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

  @Test
  public void testStripsAdjustPureNegativeAtEveryNestingLevel() throws Exception {
    String legacy =
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.boolQuery()
                    .must(
                        QueryBuilders.boolQuery()
                            .should(QueryBuilders.termQuery("entityType", "dataset"))
                            .should(QueryBuilders.existsQuery("removed"))))
            .toString();
    // OpenSearch's bool builder always serializes this internal Lucene default at every level.
    assertTrue(legacy.contains(ADJUST_PURE_NEGATIVE));

    String normalized = LegacyRangeQueryNormalizer.normalize(legacy, objectMapper);

    assertFalse(normalized.contains(ADJUST_PURE_NEGATIVE));
  }

  /**
   * Reproduces the filtered kNN semantic-search failure on Elasticsearch 8 backends. The deeply
   * nested bool filter DataHub builds via OpenSearch query builders is parsed as a SearchRequest
   * body through the ES 8 typed client's single-arg {@code withJson(Reader)}, whose strict default
   * mapper rejects the unknown {@code adjust_pure_negative} field — exactly how {@code
   * Es8SearchClientShim#searchKnn} parses its body. Parsing (not string absence) is what proves the
   * fix and that no other legacy field trips the strict parser. (The regular search path uses the
   * two-arg {@code withJson(parser, JacksonJsonpMapper)} form, which is lenient — so this bug only
   * surfaces on the kNN path.)
   */
  @Test
  public void testNormalizedNestedBoolParsesWithElasticsearch8TypedClient() throws Exception {
    String body =
        "{\"query\":"
            + QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.boolQuery()
                        .must(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.termQuery("entityType", "dataset"))
                                .should(QueryBuilders.existsQuery("removed"))))
                .toString()
            + "}";

    // Raw body is rejected by the ES 8 typed parser on adjust_pure_negative, as searchKnn saw it
    boolean rawRejected = false;
    try {
      parseSearchBodyWithElasticsearch8(body);
    } catch (Exception e) {
      rawRejected = true;
    }
    assertTrue(rawRejected, "Expected raw OpenSearch bool JSON to be rejected by the ES 8 parser");

    // After normalization the same body parses without error.
    parseSearchBodyWithElasticsearch8(LegacyRangeQueryNormalizer.normalize(body, objectMapper));
  }

  /** Mirrors {@code Es8SearchClientShim#searchKnn}'s single-arg strict body parse. */
  private void parseSearchBodyWithElasticsearch8(String bodyJson) {
    co.elastic.clients.elasticsearch.core.SearchRequest.of(
        b -> b.withJson(new StringReader(bodyJson)));
  }
}
