package com.linkedin.metadata.utils.elasticsearch.shim;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class SemanticIndexSpecTest {

  @Test
  public void buildsSpec() {
    SemanticIndexSpec s =
        SemanticIndexSpec.builder()
            .indexName("dataset_v2_semantic")
            .modelKey("gemini_embedding_001")
            .vectorDimension(768)
            .similarity("cosine")
            .hnswM(16)
            .hnswEfConstruction(128)
            .build();

    assertEquals(s.indexName(), "dataset_v2_semantic");
    assertEquals(s.modelKey(), "gemini_embedding_001");
    assertEquals(s.vectorDimension(), 768);
    assertEquals(s.similarity(), "cosine");
    assertEquals(s.hnswM(), 16);
    assertEquals(s.hnswEfConstruction(), 128);
  }
}
