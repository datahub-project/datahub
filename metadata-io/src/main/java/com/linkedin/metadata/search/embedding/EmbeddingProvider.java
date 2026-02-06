package com.linkedin.metadata.search.embedding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Provides query embeddings for semantic search. Implementations may call an external service. */
public interface EmbeddingProvider {

  /**
   * Returns an embedding vector for the given text using the specified model. The dimensionality of
   * the returned vector is determined by the model.
   *
   * @param text The text to embed
   * @param model The model identifier (e.g., "cohere.embed-english-v3"). If null, uses the
   *     provider's default model.
   * @return The embedding vector with dimensions determined by the model
   * @throws RuntimeException if embedding generation fails
   */
  @Nonnull
  float[] embed(@Nonnull String text, @Nullable String model);
}
