package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link OpenSearch2SearchClientShim#parseSearchKnnResponse(JsonNode, ObjectMapper)}
 * and related kNN search behaviour. We test the static response-parsing method directly to avoid
 * needing a live OpenSearch cluster.
 */
public class OpenSearch2SearchKnnTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode parse(String json) throws Exception {
    return MAPPER.readTree(json);
  }

  @Test
  public void parsesHitsWithCorrectIdsAndScores() throws Exception {
    String responseJson =
        "{"
            + "\"hits\":{"
            + "  \"hits\":["
            + "    {\"_id\":\"urn:li:dataset:abc\",\"_score\":0.95,\"_source\":{\"urn\":\"urn:li:dataset:abc\"}},"
            + "    {\"_id\":\"urn:li:dataset:xyz\",\"_score\":0.88,\"_source\":{}}"
            + "  ]"
            + "}"
            + "}";

    KnnSearchResponse response =
        OpenSearch2SearchClientShim.parseSearchKnnResponse(parse(responseJson), MAPPER);

    assertFalse(response.isEmpty());
    assertEquals(response.hits().size(), 2);
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:abc");
    assertEquals(response.hits().get(0).score(), 0.95, 0.001);
    assertEquals(response.hits().get(1).id(), "urn:li:dataset:xyz");
    assertEquals(response.hits().get(1).score(), 0.88, 0.001);
  }

  @Test
  public void returnsEmptyResponseForNoHits() throws Exception {
    String responseJson = "{\"hits\":{\"hits\":[]}}";
    KnnSearchResponse response =
        OpenSearch2SearchClientShim.parseSearchKnnResponse(parse(responseJson), MAPPER);
    assertTrue(response.isEmpty());
    assertEquals(response.hits().size(), 0);
  }

  @Test
  public void skipsHitsWithEmptyId() throws Exception {
    String responseJson =
        "{"
            + "\"hits\":{"
            + "  \"hits\":["
            + "    {\"_id\":\"\",\"_score\":0.99,\"_source\":{\"urn\":\"broken\"}},"
            + "    {\"_id\":\"urn:li:dataset:ok\",\"_score\":0.80,\"_source\":{}}"
            + "  ]"
            + "}"
            + "}";

    KnnSearchResponse response =
        OpenSearch2SearchClientShim.parseSearchKnnResponse(parse(responseJson), MAPPER);

    assertEquals(response.hits().size(), 1, "Hit with empty _id must be skipped");
    assertEquals(response.hits().get(0).id(), "urn:li:dataset:ok");
  }

  @Test
  public void missingSourceFallsBackToEmptyMap() throws Exception {
    String responseJson =
        "{"
            + "\"hits\":{"
            + "  \"hits\":["
            + "    {\"_id\":\"urn:li:dataset:nosrc\",\"_score\":0.70}"
            + "  ]"
            + "}"
            + "}";

    KnnSearchResponse response =
        OpenSearch2SearchClientShim.parseSearchKnnResponse(parse(responseJson), MAPPER);

    assertEquals(response.hits().size(), 1);
    Map<String, Object> source = response.hits().get(0).source();
    assertNotNull(source);
    assertTrue(source.isEmpty(), "Missing _source should fall back to empty map");
  }

  @Test
  public void scoreDefaultsToZeroWhenMissing() throws Exception {
    String responseJson =
        "{"
            + "\"hits\":{"
            + "  \"hits\":["
            + "    {\"_id\":\"urn:li:dataset:noscore\",\"_source\":{}}"
            + "  ]"
            + "}"
            + "}";

    KnnSearchResponse response =
        OpenSearch2SearchClientShim.parseSearchKnnResponse(parse(responseJson), MAPPER);

    assertEquals(response.hits().size(), 1);
    assertEquals(response.hits().get(0).score(), 0.0, 0.001, "Missing _score should default to 0");
  }
}
