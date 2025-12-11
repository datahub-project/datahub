/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.models.graph;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Data
public class RelatedEntity {
  /** How the entity is related, along which edge. */
  protected final String relationshipType;

  /** Urn associated with the related entity. */
  protected final String urn;

  /** Urn associated with an entity through which this relationship is established */
  protected final String via;

  /**
   * Constructor for backwards compatibility
   *
   * @param relationshipType
   * @param urn
   */
  public RelatedEntity(String relationshipType, String urn) {
    this(relationshipType, urn, null);
  }
}
