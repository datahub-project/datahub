package com.linkedin.metadata.entity;

/** Result of a ceiling-bounded hard delete. */
public enum ConditionalDeleteOutcome {
  /**
   * Everything at or below the ceiling was deleted and nothing newer remained: for an entity the
   * key went too (the entity is gone); for an aspect every version went.
   */
  DELETED,
  /**
   * Rows at or below the ceiling were deleted (possibly none) but newer data survives: for an
   * entity at least one aspect advanced past its ceiling or was created after the capture, so the
   * key stays; for an aspect its latest version is newer than the ceiling.
   */
  PARTIAL,
  /**
   * Nothing to delete: the key (or the aspect) is absent, or the urn was hard-deleted and recreated
   * after the capture. Nothing was written and no MCL was produced.
   */
  ALREADY_DELETED
}
