package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticVectorState;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Read rules for embedding-profile state, including legacy data that predates the fields. */
public final class SemanticProfileStates {

  private SemanticProfileStates() {}

  /**
   * The stored vector state. A missing field with chunks is ready. A missing field with no chunks
   * is absent, which is an empty optional. A key that also has a work state and no vectors is a
   * failed first embed: the optional is empty and the work state is still set.
   */
  @Nonnull
  public static Optional<SemanticVectorState> vectorState(@Nonnull EmbeddingModelData profile) {
    if (profile.hasVectorState()) {
      return Optional.of(profile.getVectorState());
    }
    if (profile.hasChunks() && !profile.getChunks().isEmpty()) {
      return Optional.of(SemanticVectorState.READY);
    }
    return Optional.empty();
  }
}
