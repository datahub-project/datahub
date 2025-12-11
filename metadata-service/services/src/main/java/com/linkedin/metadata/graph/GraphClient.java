/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.graph;

import com.linkedin.common.EntityRelationships;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface GraphClient {
  public static final Integer DEFAULT_PAGE_SIZE = 100;

  /**
   * Returns a list of related entities for a given entity, set of edge types, and direction
   * relative to the source node
   */
  @Nonnull
  EntityRelationships getRelatedEntities(
      String rawUrn,
      Set<String> relationshipTypes,
      RelationshipDirection direction,
      @Nullable Integer start,
      @Nullable Integer count,
      String actor);

  /**
   * Returns lineage relationships for given entity in the DataHub graph. Lineage relationship
   * denotes whether an entity is directly upstream or downstream of another entity
   */
  @Nonnull
  EntityLineageResult getLineageEntities(
      String rawUrn,
      LineageDirection direction,
      @Nullable Integer start,
      @Nullable Integer count,
      int maxHops,
      String actor);
}
