package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.common.SemanticContent;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Primary-storage reads and a profile-scoped patch of {@code semanticContent}. */
public interface SemanticContentStore {

  boolean exists(@Nonnull OperationContext context, @Nonnull Urn urn);

  @Nullable
  SemanticContent get(@Nonnull OperationContext context, @Nonnull Urn urn);

  /**
   * Writes the patch. Returns false when one of its profile preconditions no longer matches the
   * stored aspect or the write was not committed; the caller re-reads and retries its own key.
   */
  boolean patch(
      @Nonnull OperationContext context, @Nonnull Urn urn, @Nonnull MetadataChangeProposal patch);
}
