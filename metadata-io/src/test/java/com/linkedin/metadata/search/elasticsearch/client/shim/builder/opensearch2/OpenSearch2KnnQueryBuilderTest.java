package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import static org.testng.Assert.*;

import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class OpenSearch2KnnQueryBuilderTest {

  private static final String VECTOR_FIELD = "embeddings.text_embedding_3_large.chunks.vector";
  private static final String NESTED_PATH = "embeddings.text_embedding_3_large.chunks";

  private KnnSearchRequest baseRequest() {
    return KnnSearchRequest.builder()
        .indexName("datasetindex_v2_semantic")
        .vectorField(VECTOR_FIELD)
        .queryVector(new float[] {0.1f, 0.2f, 0.3f})
        .k(10)
        .build();
  }

  @Test
  public void testTopLevelBodyShape() {
    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(baseRequest());

    assertTrue(body.containsKey("size"), "Body should have size key");
    assertEquals(body.get("size"), 10, "size should equal k");
    assertEquals(body.get("track_total_hits"), false, "track_total_hits should be false");
    // baseRequest has no fieldsToFetch; _source should be omitted (not empty array)
    assertFalse(
        body.containsKey("_source"), "_source should be absent when fieldsToFetch is empty");
    assertTrue(body.containsKey("query"), "Body should have query key");
  }

  @Test
  public void testSourceOmittedWhenFieldsToFetchIsEmpty() {
    // Explicitly verify that empty fieldsToFetch omits _source entirely
    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(baseRequest());
    assertFalse(
        body.containsKey("_source"),
        "_source must be absent when fieldsToFetch is empty so the server returns full source");
  }

  @Test
  public void testSourcePresentWhenFieldsToFetchIsSet() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(VECTOR_FIELD)
            .queryVector(new float[] {0.1f, 0.2f})
            .k(5)
            .fieldsToFetch(java.util.List.of("urn", "name"))
            .build();

    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(req);
    assertTrue(body.containsKey("_source"), "_source should be present when fieldsToFetch is set");
  }

  @Test
  public void testNestedQueryStructure() {
    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(baseRequest());

    @SuppressWarnings("unchecked")
    Map<String, Object> outerQuery = (Map<String, Object>) body.get("query");
    assertTrue(outerQuery.containsKey("nested"), "query should wrap nested");

    @SuppressWarnings("unchecked")
    Map<String, Object> nested = (Map<String, Object>) outerQuery.get("nested");
    assertEquals(nested.get("path"), NESTED_PATH, "path should strip trailing .vector");
    assertEquals(nested.get("score_mode"), "max", "score_mode should be max");
    assertTrue(nested.containsKey("query"), "nested should have inner query");

    @SuppressWarnings("unchecked")
    Map<String, Object> innerQuery = (Map<String, Object>) nested.get("query");
    assertTrue(innerQuery.containsKey("knn"), "inner query should have knn key");

    @SuppressWarnings("unchecked")
    Map<String, Object> knnMap = (Map<String, Object>) innerQuery.get("knn");
    assertTrue(knnMap.containsKey(VECTOR_FIELD), "knn map should be keyed by the full vectorField");

    @SuppressWarnings("unchecked")
    Map<String, Object> knnParams = (Map<String, Object>) knnMap.get(VECTOR_FIELD);
    assertTrue(knnParams.containsKey("vector"), "knn params should have vector");
    assertEquals(knnParams.get("k"), 10, "knn params k should match request k");

    // vector must be List<Float>, not float[]
    Object vectorObj = knnParams.get("vector");
    assertTrue(
        vectorObj instanceof List, "vector must be a List (not float[]) for OS serialization");
    @SuppressWarnings("unchecked")
    List<Float> vectorList = (List<Float>) vectorObj;
    assertEquals(vectorList.size(), 3, "vector list should have 3 elements");
  }

  @Test
  public void testFilterPlacedInsideKnnParams() {
    Map<String, Object> filterClause = Map.of("term", Map.of("platform", "snowflake"));
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(VECTOR_FIELD)
            .queryVector(new float[] {0.1f, 0.2f})
            .k(5)
            .filter(filterClause)
            .build();

    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(req);

    @SuppressWarnings("unchecked")
    Map<String, Object> nested =
        (Map<String, Object>) ((Map<String, Object>) body.get("query")).get("nested");

    @SuppressWarnings("unchecked")
    Map<String, Object> knnParams =
        (Map<String, Object>)
            ((Map<String, Object>) ((Map<String, Object>) nested.get("query")).get("knn"))
                .get(VECTOR_FIELD);

    assertTrue(
        knnParams.containsKey("filter"),
        "Filter must be inside knn params (not at bool level) for OS pre-filtering");
    assertEquals(
        knnParams.get("filter"), filterClause, "The filter map should be the exact one provided");
  }

  @Test
  public void testNoFilterWhenNotProvided() {
    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(baseRequest());

    @SuppressWarnings("unchecked")
    Map<String, Object> nested =
        (Map<String, Object>) ((Map<String, Object>) body.get("query")).get("nested");

    @SuppressWarnings("unchecked")
    Map<String, Object> knnParams =
        (Map<String, Object>)
            ((Map<String, Object>) ((Map<String, Object>) nested.get("query")).get("knn"))
                .get(VECTOR_FIELD);

    assertFalse(
        knnParams.containsKey("filter"), "filter should not be present when request has no filter");
  }

  @Test
  public void testNoOuterBoolQuery() {
    // OpenSearch uses nested kNN directly — not wrapped in an outer bool like ES8
    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(baseRequest());

    @SuppressWarnings("unchecked")
    Map<String, Object> outerQuery = (Map<String, Object>) body.get("query");
    assertFalse(
        outerQuery.containsKey("bool"), "OS kNN builder should not have outer bool wrapper");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extractKnnParams(
      Map<String, Object> body, String vectorField) {
    Map<String, Object> nested =
        (Map<String, Object>) ((Map<String, Object>) body.get("query")).get("nested");
    return (Map<String, Object>)
        ((Map<String, Object>) ((Map<String, Object>) nested.get("query")).get("knn"))
            .get(vectorField);
  }

  @Test
  public void testNumCandidatesGreaterThanKSetsEfSearch() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(VECTOR_FIELD)
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(10)
            .numCandidates(100)
            .build();

    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(req);
    Map<String, Object> knnParams = extractKnnParams(body, VECTOR_FIELD);

    assertTrue(
        knnParams.containsKey("method_parameters"),
        "method_parameters should be set when numCandidates > k");
    @SuppressWarnings("unchecked")
    Map<String, Object> methodParams = (Map<String, Object>) knnParams.get("method_parameters");
    assertEquals(methodParams.get("ef_search"), 100, "ef_search should equal numCandidates");
  }

  @Test
  public void testNumCandidatesEqualToKOmitsEfSearch() {
    // numCandidates == k (default) — no ef_search needed
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(VECTOR_FIELD)
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(10)
            .build(); // numCandidates defaults to k*10=100, which is > k=10

    // Actually with k=10 numCandidates defaults to 100 (>k), so let's explicitly set
    // numCandidates=k
    KnnSearchRequest reqEqualCandidates =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(VECTOR_FIELD)
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(10)
            .numCandidates(10)
            .build();

    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(reqEqualCandidates);
    Map<String, Object> knnParams = extractKnnParams(body, VECTOR_FIELD);

    assertFalse(
        knnParams.containsKey("method_parameters"),
        "method_parameters should be absent when numCandidates == k (no exploration benefit)");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidVectorFieldThrows() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.chunks.notVector")
            .queryVector(new float[] {0.1f})
            .k(1)
            .build();

    OpenSearch2KnnQueryBuilder.build(req);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMidStringDotVectorThrows() {
    // ".vector" occurs mid-string but the field does not end with ".vector"
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField("embeddings.chunks.vectordata")
            .queryVector(new float[] {0.1f})
            .k(1)
            .build();

    OpenSearch2KnnQueryBuilder.build(req);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyPrefixThrows() {
    // ".vector" is the entire field name — no nested path prefix
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("datasetindex_v2_semantic")
            .vectorField(".vector")
            .queryVector(new float[] {0.1f})
            .k(1)
            .build();

    OpenSearch2KnnQueryBuilder.build(req);
  }
}
