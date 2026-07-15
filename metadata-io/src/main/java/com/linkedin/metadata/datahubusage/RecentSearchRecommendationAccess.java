package com.linkedin.metadata.datahubusage;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Recent search queries for search-bar recommendations, independent of whether usage events are
 * read from OpenSearch or PostgreSQL.
 */
public interface RecentSearchRecommendationAccess {

  /** Whether recent-search recommendations may run (e.g. usage index or partitions exist). */
  boolean isDataAvailable(@Nonnull OperationContext opContext);

  /**
   * Search queries for the session actor, most recently used first, excluding invalid entries
   * (implementation may return extra rows; callers typically limit and filter).
   */
  @Nonnull
  List<String> recentSearchQueries(@Nonnull OperationContext opContext);
}
