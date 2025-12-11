/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.schematron.converters;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.schema.SchemaMetadata;

/** Base interface for converting between different schema formats. */
public interface SchemaConverter<T> {
  /**
   * Converts a schema into DataHub's SchemaField format.
   *
   * @param schema The source schema to convert
   * @param isKeySchema Whether this represents a key schema
   * @param defaultNullable Default nullable setting for fields
   * @param platformUrn Data platform urn
   * @param rawSchemaString Raw schema string (if available). When provided - it will be used to
   *     generate the schema fingerprint
   * @return List of SchemaFields representing the schema in DataHub's format
   */
  SchemaMetadata toDataHubSchema(
      T schema,
      boolean isKeySchema,
      boolean defaultNullable,
      DataPlatformUrn platformUrn,
      String rawSchemaString);
}
