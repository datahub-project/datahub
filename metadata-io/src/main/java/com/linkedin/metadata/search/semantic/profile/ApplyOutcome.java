package com.linkedin.metadata.search.semantic.profile;

/** Result of applying one profile. Expected drops are not logged as warnings. */
public enum ApplyOutcome {
  ACCEPTED,
  DROPPED_ENTITY_MISSING,
  DROPPED_HASH_MISMATCH,
  DROPPED_STALE_RESULT,
  DROPPED_INELIGIBLE,
  DROPPED_STALE_WORK
}
