package com.linkedin.metadata.config.search;

import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for semantic search indices using k-NN vector similarity. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SemanticSearchConfiguration {

  /** Whether semantic search indices should be created. Defaults to false. */
  private boolean enabled;

  /**
   * Set of entity names for which semantic search indices should be created. For example:
   * ["dataset", "chart"].
   */
  private Set<String> enabledEntities;

  /** Map of embedding model configurations keyed by model name. */
  private Map<String, ModelEmbeddingConfig> models;

  /** Configuration for the embedding provider used to generate query embeddings. */
  private EmbeddingProviderConfiguration embeddingProvider;
}
