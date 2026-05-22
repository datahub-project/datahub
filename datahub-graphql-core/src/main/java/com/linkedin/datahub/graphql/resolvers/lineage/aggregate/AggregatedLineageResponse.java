package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * Canonical internal response shape produced by {@link AggregatedLineageResolver}. Subclasses
 * marshal this into the appropriate auto-generated GraphQL result type ({@code DomainLineageResult}
 * or {@code DataProductLineageResult}). Keeping the algorithm result decoupled from the GraphQL
 * types avoids duplicating the algorithm per result class and lets tests assert on the canonical
 * shape.
 */
@Value
@Builder
public class AggregatedLineageResponse {

  int start;
  int count;
  int total;
  int memberScanCount;
  int memberTotal;
  boolean isPartial;

  /** Echoes the request direction; every relationship in the response shares this value. */
  LineageDirection direction;

  List<Relationship> relationships;

  @Value
  @Builder
  public static class Relationship {
    /**
     * The neighbour entity — already restricted-wrapped when appropriate. Typed as the GraphQL
     * {@link Entity} interface because it may be Domain / DataProduct / Restricted depending on
     * caller permissions and {@link AggregatedLineageRequest#isGroupByDataProduct()}.
     */
    Entity entity;

    /** How many distinct source members produced a lineage hit that bucketed into this owner. */
    int memberMatchCount;

    /** How many distinct neighbour entities in this owner bucket were touched. */
    int neighbourEntityCount;

    /** Minimum hop count across all contributing paths. */
    int degreeMin;

    /** Maximum hop count across all contributing paths. */
    int degreeMax;
  }
}
