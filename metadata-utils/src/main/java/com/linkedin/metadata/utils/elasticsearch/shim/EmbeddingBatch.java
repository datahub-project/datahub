package com.linkedin.metadata.utils.elasticsearch.shim;

import java.util.List;
import javax.annotation.Nonnull;

public record EmbeddingBatch(
    @Nonnull String indexName,
    @Nonnull String documentId,
    @Nonnull String modelKey,
    @Nonnull List<Chunk> chunks) {

  public EmbeddingBatch {
    chunks = List.copyOf(chunks);
  }

  public record Chunk(
      @Nonnull float[] vector,
      @Nonnull String text,
      int position,
      int characterOffset,
      int characterLength,
      int tokenCount) {
    public Chunk {
      vector = vector.clone();
    }
  }
}
