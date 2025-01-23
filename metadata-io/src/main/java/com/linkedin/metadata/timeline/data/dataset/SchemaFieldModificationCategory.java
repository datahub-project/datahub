package com.linkedin.metadata.timeline.data.dataset;

/*
 * Enum to allow us to distinguish between the different schema field modifications when creating entity change events.
 */
public enum SchemaFieldModificationCategory {
  // when a schema field is renamed
  RENAME,
  // when a schema field has a type change
  TYPE_CHANGE,
  // a default option when no other modification category has been given
  OTHER,
}
