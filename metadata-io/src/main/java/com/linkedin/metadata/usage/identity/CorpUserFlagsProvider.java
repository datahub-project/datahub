package com.linkedin.metadata.usage.identity;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Reads CorpUserInfo flags for usage actor classification. */
public interface CorpUserFlagsProvider {

  boolean isSystemCorpUser(@Nonnull String corpUserUrn);

  boolean isSupportUser(@Nonnull String corpUserUrn);

  @Nonnull
  default CorpUserFlags resolveWithContext(
      @Nonnull OperationContext opContext, @Nonnull String corpUserUrn) {
    return new CorpUserFlags(isSystemCorpUser(corpUserUrn), isSupportUser(corpUserUrn));
  }

  record CorpUserFlags(boolean system, boolean supportUser) {
    static final CorpUserFlags DEFAULT = new CorpUserFlags(false, false);
  }
}
