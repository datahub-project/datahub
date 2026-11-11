package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Whether a profile result may be stored. SaaS replaces the OSS bean with policy. */
public interface SemanticProfileEligibility {

  @Nonnull
  Decision evaluate(
      @Nonnull OperationContext context,
      @Nonnull Urn urn,
      @Nonnull String profileKey,
      @Nullable EmbeddingModelData proposed);

  record Eligible(@Nullable String policyVersion) implements Decision {}

  record Excluded(@Nonnull String reason, @Nullable String policyVersion) implements Decision {}

  sealed interface Decision permits Eligible, Excluded {}
}
