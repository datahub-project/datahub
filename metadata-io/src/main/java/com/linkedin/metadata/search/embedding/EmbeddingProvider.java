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

  /**
   * Returns an embedding vector for the given text, with the caller specifying whether this is for
   * indexing a document or for retrieving results (query time). Asymmetric embedding models produce
   * higher quality when the task type is provided. The default implementation ignores the task type
   * and delegates to {@link #embed(String, String)}.
   *
   * @param text The text to embed
   * @param model The model identifier. If null, uses the provider's default model.
   * @param taskType Whether this is a document-level or query-level embedding
   * @return The embedding vector
   * @throws RuntimeException if embedding generation fails
   */
  @Nonnull
  default float[] embed(
      @Nonnull String text, @Nullable String model, @Nonnull EmbeddingTaskType taskType) {
    return embed(text, model);
  }
}
