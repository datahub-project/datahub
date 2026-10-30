package com.linkedin.metadata.aspect.consistency.fix;

/**
 * Types of fixes that can be applied to resolve consistency issues.
 *
 * <p>Each fix type corresponds to a specific {@link
 * com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix} implementation.
 */
public enum ConsistencyFixType {

  /** Soft delete the entity (set status.removed = true) */
  SOFT_DELETE,

  /** Hard delete the entity (permanently remove from storage) */
  HARD_DELETE,

  /** Create a new entity or aspect */
  CREATE,

  /** Upsert (create or replace) an aspect on an existing entity */
  UPSERT,

  /** Apply a JSON patch to an aspect */
  PATCH,

  /** Delete a specific aspect from an entity (not the whole entity) */
  DELETE_ASPECT,

  /** Trim unknown fields from an aspect and upsert */
  TRIM_UPSERT,

  /** Delete orphaned documents directly from indices (system metadata, entity search, graph) */
  DELETE_INDEX_DOCUMENTS,

  /**
   * Set mapped structured-property fields that differ from SQL and null mapped fields SQL does not
   * project. No other search field is written, and SQL is left unchanged.
   */
  SYNC_STRUCTURED_PROPERTY_INDEX_FIELDS
}
