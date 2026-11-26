package com.linkedin.metadata.service;

/** What one hard delete removes; one value per {@code HardDeleteService} delete. */
public enum DeleteKind {
  /** The entity. */
  ENTITY,
  /**
   * The entity, then the references other entities hold to it. When the entity stays ({@code
   * PARTIAL}) the references are left alone, as a failed delete leaves them today.
   */
  ENTITY_AND_REFERENCES,
  /**
   * The entity, then its timeseries aspect values in a time window. When the entity stays the
   * values are left alone, as a failed delete leaves them today.
   */
  ENTITY_AND_TIMESERIES,
  /** Only the references other entities hold to the entity. */
  REFERENCES
}
