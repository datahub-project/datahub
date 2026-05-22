package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import java.util.Collections;
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

  /**
   * Inner edges that live entirely inside the source aggregation scope (e.g. DataProduct ↔
   * DataProduct edges where both DPs belong to the source Domain). Default empty. Subclasses opt in
   * via {@link AggregatedLineageResolver#computeInnerEdges}.
   */
  @Builder.Default List<InnerEdge> innerEdges = Collections.emptyList();

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

  /**
   * An aggregated lineage edge between two entities that both live in the source scope, e.g. two
   * DataProducts inside the same source Domain. Canonicalised to {@code (upstream, downstream)} on
   * the server so the frontend can dedupe without re-deriving direction.
   */
  @Value
  @Builder
  public static class InnerEdge {
    Entity upstream;
    Entity downstream;

    /**
     * Count of distinct (sourceMember, neighbour) asset-level paths that contributed to this edge.
     * Multiple pairs collapse into a single edge; this is the contributing pair count.
     */
    int memberMatchCount;

    int degreeMin;
    int degreeMax;
  }
}
