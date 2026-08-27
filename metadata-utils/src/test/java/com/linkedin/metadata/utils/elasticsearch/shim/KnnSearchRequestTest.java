package com.linkedin.metadata.utils.elasticsearch.shim;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class KnnSearchRequestTest {

  @Test
  public void buildsMinimalRequest() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("dataset_v2_semantic")
            .vectorField("embeddings.gemini.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f, 0.3f})
            .k(10)
            .numCandidates(100)
            .fieldsToFetch(List.of("urn"))
            .build();

    assertEquals(req.indexName(), "dataset_v2_semantic");
    assertEquals(req.vectorField(), "embeddings.gemini.chunks.vector");
    assertEquals(req.k(), 10);
    assertEquals(req.numCandidates(), 100);
    assertEquals(req.queryVector().length, 3);
    assertFalse(req.filter().isPresent());
  }

  @Test(
      expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*queryVector.*")
  public void rejectsMissingVector() {
    KnnSearchRequest.builder().indexName("idx").vectorField("vec").k(10).build();
  }

  @Test(
      expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*k must be >= 1.*")
  public void rejectsKLessThanOne() {
    KnnSearchRequest.builder()
        .indexName("idx")
        .vectorField("vec")
        .queryVector(new float[] {0.1f})
        .k(0)
        .build();
  }

  @Test(
      expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*indexName and vectorField are required.*")
  public void rejectsMissingIndexName() {
    KnnSearchRequest.builder()
        .vectorField("embeddings.v.chunks.vector")
        .queryVector(new float[] {0.1f})
        .k(5)
        .build();
  }

  @Test
  public void numCandidatesDefaultsWhenNotSet() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("idx")
            .vectorField("embeddings.v.chunks.vector")
            .queryVector(new float[] {0.1f})
            .k(7)
            .build();

    // numCandidates should default to k * 10 = 70
    assertEquals(req.numCandidates(), 70);
  }

  @Test
  public void filterReturnsPresentWhenSet() {
    Map<String, Object> filterClause = Map.of("term", Map.of("platform", "snowflake"));
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("idx")
            .vectorField("embeddings.v.chunks.vector")
            .queryVector(new float[] {0.1f})
            .k(5)
            .filter(filterClause)
            .build();

    assertTrue(req.filter().isPresent(), "filter() should be present when filter was set");
    assertEquals(req.filter().get().get("term"), Map.of("platform", "snowflake"));
  }

  @Test
  public void filterReturnsEmptyWhenNotSet() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("idx")
            .vectorField("embeddings.v.chunks.vector")
            .queryVector(new float[] {0.1f})
            .k(5)
            .build();

    assertFalse(req.filter().isPresent(), "filter() should be empty when filter was not set");
  }

  @Test
  public void fieldsToFetchEmptyWhenNotSet() {
    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("idx")
            .vectorField("embeddings.v.chunks.vector")
            .queryVector(new float[] {0.1f})
            .k(5)
            .build();

    assertTrue(req.fieldsToFetch().isEmpty(), "fieldsToFetch should be empty when not set");
  }

  @Test
  public void filterAllowsNullValues() {
    // Filter maps can contain null values to express "missing field" semantics in ES/OS DSL.
    // Map.copyOf rejects null values, so the builder must use a defensive HashMap copy instead.
    Map<String, Object> filterWithNull = new HashMap<>();
    filterWithNull.put("platform", "snowflake");
    filterWithNull.put("optional_field", null);

    KnnSearchRequest req =
        KnnSearchRequest.builder()
            .indexName("idx")
            .vectorField("embeddings.v.chunks.vector")
            .queryVector(new float[] {0.1f})
            .k(5)
            .filter(filterWithNull)
            .build();

    assertTrue(req.filter().isPresent(), "filter() should be present");
    assertEquals(req.filter().get().get("platform"), "snowflake");
    assertNull(req.filter().get().get("optional_field"), "null filter value should be preserved");
    // The returned filter map must be unmodifiable.
    assertThrows(UnsupportedOperationException.class, () -> req.filter().get().put("x", "y"));
  }
}
