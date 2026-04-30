package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import static org.testng.Assert.*;

import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class Es8KnnQueryBuilderTest {

  private KnnSearchRequest baseRequest() {
    return KnnSearchRequest.builder()
        .indexName("datasetindex_v2_semantic")
        .vectorField("embeddings.gemini_embedding_001.chunks.vector")
        .queryVector(new float[] {0.1f, 0.2f, 0.3f})
        .k(10)
        .build();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extractBool(Map<String, Object> body) {
    Map<String, Object> query = (Map<String, Object>) body.get("query");
    return (Map<String, Object>) query.get("bool");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extractNested(Map<String, Object> body) {
    Map<String, Object> bool = extractBool(body);
    List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");
    return (Map<String, Object>) must.get(0).get("nested");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extractKnnInner(Map<String, Object> body) {
    Map<String, Object> nested = extractNested(body);
    Map<String, Object> innerQuery = (Map<String, Object>) nested.get("query");
    return (Map<String, Object>) innerQuery.get("knn");
  }

  @Test
  public void testNestedKnnQueryStructure() {
    Map<String, Object> body = Es8KnnQueryBuilder.build(baseRequest());

    assertTrue(body.containsKey("query"), "Body should have query key");

    @SuppressWarnings("unchecked")
    Map<String, Object> query = (Map<String, Object>) body.get("query");
    // Filters go in outer bool.filter to support root-level doc fields (platform, urn, entityType)
    assertTrue(query.containsKey("bool"), "query should wrap bool for root-level filter support");
    assertFalse(query.containsKey("nested"), "nested is inside bool.must, not at query root");

    Map<String, Object> bool = extractBool(body);
    assertTrue(bool.containsKey("must"), "bool should have must clause");

    Map<String, Object> nested = extractNested(body);
    assertEquals(
        nested.get("path"),
        "embeddings.gemini_embedding_001.chunks",
        "nested path should strip trailing .vector");

    @SuppressWarnings("unchecked")
    Map<String, Object> innerQuery = (Map<String, Object>) nested.get("query");
    assertTrue(innerQuery.containsKey("knn"), "nested query should have knn key");

    Map<String, Object> knnInner = extractKnnInner(body);
    assertEquals(
        knnInner.get("field"),
        "embeddings.gemini_embedding_001.chunks.vector",
        "knn field should be the full vectorField");
    assertNotNull(knnInner.get("query_vector"), "knn should have query_vector");
    assertEquals(knnInner.get("k"), 10, "knn k should match request k");
    assertTrue(knnInner.containsKey("num_candidates"), "knn should have num_candidates");
  }

  @Test
  public void testFilterPlacedInOuterBoolForRootFieldAccess() {
    // Root-level filters (platform, urn, entityType, _index) cannot be referenced inside a nested
    // kNN clause in ES 8 — they must live in the outer bool.filter.
    Map<String, Object> filterClause = Map.of("term", Map.of("platform", "snowflake"));
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f})
            .k(5)
            .filter(filterClause)
            .build();

    Map<String, Object> body = Es8KnnQueryBuilder.build(req);

    // Filter must be in outer bool.filter, NOT inside knn clause
    Map<String, Object> bool = extractBool(body);
    assertTrue(
        bool.containsKey("filter"), "filter must be in outer bool.filter for root-field access");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> filterList = (List<Map<String, Object>>) bool.get("filter");
    assertEquals(
        filterList.get(0), filterClause, "The filter map should be the exact one provided");

    // Confirm filter is NOT inside knn inner block
    Map<String, Object> knnInner = extractKnnInner(body);
    assertFalse(
        knnInner.containsKey("filter"), "filter must NOT be inside knn clause (nested context)");
  }

  @Test
  public void testNoFilterWhenNotProvided() {
    Map<String, Object> body = Es8KnnQueryBuilder.build(baseRequest());
    Map<String, Object> bool = extractBool(body);
    assertFalse(
        bool.containsKey("filter"), "filter should not be present in bool when not provided");
    Map<String, Object> knnInner = extractKnnInner(body);
    assertFalse(
        knnInner.containsKey("filter"), "filter should not be present in knn when not provided");
  }

  @Test
  public void testNumCandidatesDefaultsToTenTimesK() {
    KnnSearchRequest req = baseRequest(); // k=10, numCandidates not set → defaults to 100
    Map<String, Object> body = Es8KnnQueryBuilder.build(req);
    Map<String, Object> knnInner = extractKnnInner(body);
    assertEquals(knnInner.get("num_candidates"), 100, "num_candidates should default to k * 10");
  }

  @Test
  public void testExplicitNumCandidatesIsUsed() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f})
            .k(5)
            .numCandidates(42)
            .build();

    Map<String, Object> body = Es8KnnQueryBuilder.build(req);
    Map<String, Object> knnInner = extractKnnInner(body);
    assertEquals(knnInner.get("num_candidates"), 42, "explicit numCandidates should be used");
  }

  @Test
  public void testFieldsToFetchPopulatedInSource() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.gemini_embedding_001.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f})
            .k(5)
            .fieldsToFetch(List.of("urn", "name"))
            .build();

    Map<String, Object> body = Es8KnnQueryBuilder.build(req);

    assertTrue(
        body.containsKey("_source"), "Body should contain _source when fieldsToFetch is set");
    assertEquals(body.get("_source"), List.of("urn", "name"), "_source should match fieldsToFetch");
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Expected vectorField to end with .vector.*")
  public void testInvalidVectorFieldThrows() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.gemini_embedding_001.chunks.embedding")
            .queryVector(new float[] {0.1f})
            .k(5)
            .build();

    Es8KnnQueryBuilder.build(req);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMidStringDotVectorThrows() {
    // ".vector" occurs mid-string but the field does not end with ".vector"
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.chunks.vectordata")
            .queryVector(new float[] {0.1f})
            .k(5)
            .build();

    Es8KnnQueryBuilder.build(req);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyPrefixThrows() {
    // ".vector" is the entire field name — no nested path prefix
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(".vector")
            .queryVector(new float[] {0.1f})
            .k(5)
            .build();

    Es8KnnQueryBuilder.build(req);
  }
}
