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
 * Canonical, server-clamped request shape consumed by {@link AggregatedLineageResolver}. Subclasses
 * translate the auto-generated GraphQL input types into this once, so the shared algorithm doesn't
 * have to handle two near-identical input classes.
 */
@Value
@Builder(toBuilder = true)
public class AggregatedLineageRequest {

  Urn sourceUrn;
  LineageDirection direction;
  int hops;
  int start;
  int count;
  int perMemberCount;
  int memberScanCap;

  /**
   * Only meaningful for Domain sources: re-bucket neighbours by their owning DataProduct instead of
   * their owning Domain.
   */
  boolean groupByDataProduct;

  /** When true, neighbours the caller cannot view become Restricted placeholders. */
  boolean includeRestricted;

  @Nullable SearchFlags searchFlags;
  @Nullable LineageFlags lineageFlags;
  @Nullable List<AndFilterInput> orFilters;
}
