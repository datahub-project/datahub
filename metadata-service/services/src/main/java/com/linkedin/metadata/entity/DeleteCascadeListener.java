package com.linkedin.metadata.entity;

import javax.annotation.Nonnull;

/**
 * The per-page hook of {@link DeleteEntityService#removeReferencesResumable}. The cascade owns the
 * loop; the listener may record where it is and may stop it. Correctness never depends on it:
 * running the cascade again from scratch is always safe.
 */
@FunctionalInterface
public interface DeleteCascadeListener {

  /** Records nothing and never stops the cascade. */
  DeleteCascadeListener NOOP = current -> {};

  /**
   * Called before each page of a phase is read. Throwing stops the cascade before that page: the
   * exception propagates unchanged.
   *
   * @param current the phase the page belongs to; a later call resuming from it skips the phases
   *     before it
   */
  void onPage(@Nonnull DeleteCascadeCheckpoint current);
}
