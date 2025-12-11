/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
