package com.linkedin.metadata.search.semantic.profile;

import javax.annotation.Nonnull;

/** Which embedding profiles exist, which are service-owned, and what content dirties each one. */
public interface SemanticProfileRegistry {

  boolean generationEnabled();

  boolean isServiceProfile(@Nonnull String entityType, @Nonnull String profileKey);

  /**
   * Binding for a stored or configured profile key. Never null; external when nothing dirties it.
   */
  @Nonnull
  ContentBinding bindingFor(@Nonnull String entityType, @Nonnull String profileKey);
}
