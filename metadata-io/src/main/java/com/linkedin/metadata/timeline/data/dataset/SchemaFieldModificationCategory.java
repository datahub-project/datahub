/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
