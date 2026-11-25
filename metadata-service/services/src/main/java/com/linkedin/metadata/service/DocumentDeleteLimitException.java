package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * The live subtree of {@code rootUrn} exceeds {@link DocumentDeleteResult#MAX_DESCENDANTS} or
 * {@link DocumentDeleteResult#MAX_DEPTH}. No status proposal was built.
 */
public class DocumentDeleteLimitException extends RuntimeException {

  public DocumentDeleteLimitException(@Nonnull Urn rootUrn, @Nonnull String reason) {
    super(String.format("Refusing to delete document %s: %s", rootUrn, reason));
  }
}
