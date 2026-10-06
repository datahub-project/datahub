package com.linkedin.metadata.search.hybrid;

import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Generates query embeddings for V3 hybrid retrieval using the configured semantic model. */
public class HybridQueryEmbeddingService {

  private final EmbeddingProvider embeddingProvider;
  private final String modelId;
  private final String modelEmbeddingKey;
  private final int expectedDimension;

  public HybridQueryEmbeddingService(
      @Nonnull final EmbeddingProvider embeddingProvider,
      @Nullable final String modelId,
      @Nonnull final String modelEmbeddingKey) {
    this(embeddingProvider, modelId, modelEmbeddingKey, 0);
  }

  /**
   * @param expectedDimension configured mapping dimension for the model; 0 disables the check
   */
  public HybridQueryEmbeddingService(
      @Nonnull final EmbeddingProvider embeddingProvider,
      @Nullable final String modelId,
      @Nonnull final String modelEmbeddingKey,
      final int expectedDimension) {
    this.embeddingProvider = embeddingProvider;
    this.modelId = modelId;
    this.modelEmbeddingKey = modelEmbeddingKey;
    this.expectedDimension = expectedDimension;
  }

  @Nonnull
  public QueryEmbedding embed(@Nonnull final String query) {
    final float[] vector = embeddingProvider.embed(query, modelId, EmbeddingTaskType.QUERY);
    if (expectedDimension > 0 && vector.length != expectedDimension) {
      throw new IllegalStateException(
          "Embedding provider returned "
              + vector.length
              + " dimensions for model '"
              + modelEmbeddingKey
              + "'; configured mapping expects "
              + expectedDimension);
    }
    return new QueryEmbedding(modelEmbeddingKey, vector);
  }

  @Nonnull
  public String getModelEmbeddingKey() {
    return modelEmbeddingKey;
  }

  @Nullable
  public String getModelId() {
    return modelId;
  }

  public record QueryEmbedding(@Nonnull String modelEmbeddingKey, @Nonnull float[] vector) {
    public QueryEmbedding {
      vector = vector.clone();
    }

    @Nonnull
    @Override
    public float[] vector() {
      return vector.clone();
    }
  }
}
