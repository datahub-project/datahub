package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for a specific embedding model's k-NN vector settings. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ModelEmbeddingConfig {

  /** Dimensionality of the embedding vectors for this model. */
  private int vectorDimension = 1024;

  /**
   * k-NN engine to use. Options: "faiss", "nmslib", "lucene". Defaults to "faiss" for best
   * performance with large datasets.
   */
  private String knnEngine = "faiss";

  /**
   * Distance metric for vector similarity. Options: "cosinesimil", "l2", "l1", "linf". Defaults to
   * "cosinesimil" which is standard for text embeddings.
   */
  private String spaceType = "cosinesimil";

  /** HNSW ef_construction parameter. Higher values improve index quality but slow down indexing. */
  private int efConstruction = 128;

  /**
   * HNSW m parameter (number of bidirectional links). Higher values improve recall but increase
   * index size.
   */
  private int m = 16;
}
