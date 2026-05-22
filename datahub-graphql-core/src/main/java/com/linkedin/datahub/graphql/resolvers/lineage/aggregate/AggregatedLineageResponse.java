package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * Canonical response shape produced by {@link AggregatedLineageResolver}. Subclasses marshal this
 * into the appropriate auto-generated GraphQL result type.
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
  LineageDirection direction;
  List<Relationship> relationships;

  @Value
  @Builder
  public static class Relationship {
    /** Already restricted-wrapped when the caller cannot view the underlying owner. */
    Entity entity;

    /** Distinct source members that produced a hit bucketed into this owner. */
    int memberMatchCount;

    /** Distinct neighbour entities in this owner's bucket. */
    int neighbourEntityCount;

    int degreeMin;
    int degreeMax;
  }
}
