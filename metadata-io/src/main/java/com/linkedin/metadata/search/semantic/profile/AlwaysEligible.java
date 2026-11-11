package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** OSS default. Every profile result is eligible. */
public class AlwaysEligible implements SemanticProfileEligibility {

  @Nonnull
  @Override
  public Decision evaluate(
      @Nonnull OperationContext context,
      @Nonnull Urn urn,
      @Nonnull String profileKey,
      @Nullable EmbeddingModelData proposed) {
    return new Eligible(null);
  }
}
