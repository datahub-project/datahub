package com.linkedin.metadata.entity;

/** Result of a version-bounded hard delete of an entity. */
public enum ConditionalDeleteOutcome {
  /** Everything captured was deleted and nothing newer remained, so the key went too. */
  DELETED,
  /** What was captured was deleted, but data written since survives, so the entity stays. */
  PARTIAL,
  /** The entity did not exist when the delete was requested. Nothing was written. */
  ALREADY_DELETED
}
