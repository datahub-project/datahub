package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;

/**
 * Base interface for aspect field specs. Contains a) the path to the field and b) the schema of the
 * field
 */
public interface FieldSpec {

  /** Returns the {@link PathSpec} corresponding to the field, relative to its parent aspect. */
  PathSpec getPath();

  /** Returns the {@link DataSchema} associated with the aspect field. */
  DataSchema getPegasusSchema();
}
