package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.LineageFlags;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Canonical internal request shape consumed by {@link AggregatedLineageResolver}. Subclasses
 * translate their auto-generated GraphQL input types ({@code DomainLineageInput} or {@code
 * DataProductLineageInput}) into this once-validated, server-clamped form so the shared algorithm
 * never has to deal with two near-identical input classes.
 */
@Value
@Builder(toBuilder = true)
public class AggregatedLineageRequest {

  /** The source Domain or DataProduct URN whose member lineage is being aggregated. */
  Urn sourceUrn;

  /** Whether to walk upstream or downstream from each member. */
  LineageDirection direction;

  /** Maximum lineage hops per member. Already clamped against {@code graph.lineageMaxHops}. */
  int hops;

  /** Page offset into the aggregated neighbour list. */
  int start;

  /** Page size for aggregated neighbours; already clamped. */
  int count;

  /** Per-member {@code searchAcrossLineage} page size; already clamped. */
  int perMemberCount;

  /** Hard cap on members enumerated; already clamped. */
  int memberScanCap;

  /**
   * If true (only meaningful for Domain sources), neighbours are re-bucketed by their owning
   * DataProduct instead of their owning Domain.
   */
  boolean groupByDataProduct;

  /** If true, neighbours the caller cannot view become Restricted placeholders. */
  boolean includeRestricted;

  @Nullable SearchFlags searchFlags;

  @Nullable LineageFlags lineageFlags;

  @Nullable List<AndFilterInput> orFilters;
}
