/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.models.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaKey extends DatabaseKey {
  private String schema;

  private static final String SCHEMA_MAP_FIELD = "schema";

  @Override
  public Map<String, String> guidDict() {
    // Get the parent's GUID dictionary first
    Map<String, String> bag = super.guidDict();

    // Add the database field if it's not null
    if (schema != null) {
      bag.put(SCHEMA_MAP_FIELD, schema);
    }

    return bag;
  }
}
