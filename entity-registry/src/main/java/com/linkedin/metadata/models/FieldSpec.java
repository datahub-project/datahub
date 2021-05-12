package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;


/**
 * Base interface for field specs. Contains the path to the field and the schema of the field
 */
public interface FieldSpec {
  PathSpec getPath();

  DataSchema getPegasusSchema();
}
