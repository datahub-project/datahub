package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import javax.annotation.Nonnull;

/** Shared construction of a profile that satisfies the aspect schema. */
public final class SemanticProfiles {

  private SemanticProfiles() {}

  /** A profile with the required fields and no vectors. */
  @Nonnull
  public static EmbeddingModelData baseProfile(@Nonnull String modelVersion, long generatedAt) {
    return new EmbeddingModelData()
        .setModelVersion(modelVersion)
        .setGeneratedAt(generatedAt)
        .setTotalChunks(0)
        .setChunks(new EmbeddingChunkArray());
  }
}
