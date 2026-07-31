package com.linkedin.metadata.utils.elasticsearch.shim;

import static org.testng.Assert.assertEquals;

import java.util.List;
import org.testng.annotations.Test;

public class EmbeddingBatchTest {

  @Test
  public void buildsBatch() {
    EmbeddingBatch.Chunk c =
        new EmbeddingBatch.Chunk(new float[] {0.1f, 0.2f}, "hello", 0, 5, 5, 1);
    EmbeddingBatch b =
        new EmbeddingBatch("dataset_v2_semantic", "urn:doc:1", "gemini_embedding_001", List.of(c));

    assertEquals(b.indexName(), "dataset_v2_semantic");
    assertEquals(b.documentId(), "urn:doc:1");
    assertEquals(b.chunks().size(), 1);
    assertEquals(b.chunks().get(0).text(), "hello");
  }
}
