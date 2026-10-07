package com.linkedin.metadata.search.hybrid;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Generates query embeddings for V3 hybrid retrieval using the configured semantic model. */
public class HybridQueryEmbeddingService {

  private final EmbeddingProvider embeddingProvider;
  private final String modelId;
  private final String modelEmbeddingKey;
  private final int expectedDimension;

  // A search page and its facet request embed the same query; repeat queries skip the provider
  private final Cache<String, float[]> recentEmbeddings =
      CacheBuilder.newBuilder().maximumSize(1_000).expireAfterWrite(Duration.ofMinutes(1)).build();

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

  /**
   * Embeds the query, or reuses a recent embedding of it.
   *
   * @param deadlineNanos {@link System#nanoTime()} by which the provider call has to end
   */
  @Nonnull
  public QueryEmbedding embed(@Nonnull final String query, final long deadlineNanos) {
    final float[] vector;
    try {
      vector = recentEmbeddings.get(query, () -> load(query, deadlineNanos));
    } catch (ExecutionException | RuntimeException e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw cause instanceof RuntimeException
          ? (RuntimeException) cause
          : new IllegalStateException("Query embedding failed", cause);
    }
    return new QueryEmbedding(modelEmbeddingKey, vector);
  }

  @Nonnull
  private float[] load(@Nonnull final String query, final long deadlineNanos) {
    final long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new UncheckedTimeoutException("The hybrid deadline passed before the query embedding");
    }
    return checkDimension(
        embeddingProvider.embed(
            query, modelId, EmbeddingTaskType.QUERY, Duration.ofNanos(remainingNanos)));
  }

  // Checked while loading, so a wrong-sized vector is not cached and the next search retries
  @Nonnull
  private float[] checkDimension(@Nonnull final float[] vector) {
    if (expectedDimension > 0 && vector.length != expectedDimension) {
      throw new IllegalStateException(
          "Embedding provider returned "
              + vector.length
              + " dimensions for model '"
              + modelEmbeddingKey
              + "'; configured mapping expects "
              + expectedDimension);
    }
    return vector;
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
