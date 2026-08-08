package com.linkedin.datahub.graphql.analytics.service;

import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Document count and per-facet counts for a single entity type, produced by a batched aggregation.
 */
@Value
public class EntityStats {
  int total;

  /** Counts keyed by facet field, holding an entry for every facet field that was requested. */
  @Nonnull Map<String, Integer> facetCounts;

  /** Documents matching {@code facetField}, or 0 if that facet was never requested. */
  public int countWithFacet(String facetField) {
    return facetCounts.getOrDefault(facetField, 0);
  }
}
