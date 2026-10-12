package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

/**
 * V2.5 query strategy for a specific tier. Each strategy builds a query of increasing breadth and
 * cost. The cascade executor calls {@link #buildQuery} for the starting tier, checks hit count, and
 * moves to the next tier on zero results.
 */
public interface QueryStrategy {

  /** The tier number (1-4) for cascade ordering. */
  int tier();

  /** Human-readable name for logging (e.g., "IDENTITY", "EXACT_NAME"). */
  @Nonnull
  String name();

  /**
   * Build the ES query for this tier.
   *
   * @param query the raw user query string
   * @param synonymMap synonym map for expansion (may be null for tiers that don't use it)
   * @return the query builder, or null if this tier cannot handle the query
   */
  @Nullable
  QueryBuilder buildQuery(
      @Nonnull final String query, @Nullable final Map<String, Set<String>> synonymMap);
}
