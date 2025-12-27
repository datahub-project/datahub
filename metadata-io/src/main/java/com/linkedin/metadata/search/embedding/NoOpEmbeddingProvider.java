package com.linkedin.metadata.search.embedding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * No-op implementation of EmbeddingProvider that throws an exception when used.
 *
 * <p>This provider is used when semantic search is not enabled in the configuration. It allows the
 * system to start without requiring embedding provider configuration, but provides a clear error
 * message if semantic search functionality is attempted.
 */
public class NoOpEmbeddingProvider implements EmbeddingProvider {

  @Nonnull
  @Override
  public float[] embed(@Nonnull String text, @Nullable String model) {
    throw new UnsupportedOperationException(
        "Semantic search is not enabled. Please configure semantic search in application.yaml "
            + "to use embedding functionality. See documentation: "
            + "https://datahubproject.io/docs/how-to/semantic-search-aws-bedrock-setup");
  }
}
