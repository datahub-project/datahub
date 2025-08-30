package com.linkedin.metadata.search.embedding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Default provider that fails fast when semantic search is not configured. */
public class DisabledEmbeddingProvider implements EmbeddingProvider {
  @Override
  public @Nonnull float[] embed(@Nonnull String text, @Nullable String model) {
    throw new IllegalStateException("Semantic search is disabled or not configured.");
  }
}
