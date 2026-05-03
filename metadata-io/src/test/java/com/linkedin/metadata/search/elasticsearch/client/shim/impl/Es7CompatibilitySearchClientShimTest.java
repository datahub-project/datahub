package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests verifying that Es7CompatibilitySearchClientShim rejects all three semantic search
 * operations with clear UnsupportedOperationException messages referencing the 8.18+ requirement.
 *
 * <p>The shim is instantiated via Mockito's CALLS_REAL_METHODS mock to bypass the constructor's
 * I/O-heavy client creation while exercising the real override implementations.
 */
public class Es7CompatibilitySearchClientShimTest {

  private Es7CompatibilitySearchClientShim shim;

  @BeforeMethod
  public void setUp() {
    // CALLS_REAL_METHODS bypasses the constructor so we can test the three semantic overrides
    // without a live OpenSearch cluster.
    shim = mock(Es7CompatibilitySearchClientShim.class, CALLS_REAL_METHODS);
  }

  @Test
  public void searchKnnThrowsUnsupportedWithCompatibilityMessage() {
    KnnSearchRequest request =
        KnnSearchRequest.builder()
            .indexName("dataset_semantic")
            .vectorField("embeddings.m.chunks.vector")
            .queryVector(new float[] {0.1f, 0.2f})
            .k(5)
            .build();

    UnsupportedOperationException ex =
        expectThrows(UnsupportedOperationException.class, () -> shim.searchKnn(request));

    String msg = ex.getMessage().toLowerCase();
    assertTrue(
        msg.contains("8.18") || msg.contains("compatibility"),
        "searchKnn error must mention 8.18 or compatibility; got: " + ex.getMessage());
  }

  @Test
  public void createSemanticIndexThrowsUnsupportedWithCompatibilityMessage() {
    SemanticIndexSpec spec =
        SemanticIndexSpec.builder()
            .indexName("dataset_semantic")
            .modelKey("m")
            .vectorDimension(4)
            .build();

    UnsupportedOperationException ex =
        expectThrows(UnsupportedOperationException.class, () -> shim.createSemanticIndex(spec));

    String msg = ex.getMessage().toLowerCase();
    assertTrue(
        msg.contains("8.18") || msg.contains("compatibility"),
        "createSemanticIndex error must mention 8.18 or compatibility; got: " + ex.getMessage());
  }

  @Test
  public void indexEmbeddingsThrowsUnsupportedWithCompatibilityMessage() {
    EmbeddingBatch batch =
        new EmbeddingBatch(
            "dataset_semantic",
            "urn:li:dataset:test",
            "m",
            List.of(new EmbeddingBatch.Chunk(new float[] {0.1f, 0.2f}, "text", 0, 0, 4, 1)));

    UnsupportedOperationException ex =
        expectThrows(UnsupportedOperationException.class, () -> shim.indexEmbeddings(batch));

    String msg = ex.getMessage().toLowerCase();
    assertTrue(
        msg.contains("8.18") || msg.contains("compatibility"),
        "indexEmbeddings error must mention 8.18 or compatibility; got: " + ex.getMessage());
  }
}
