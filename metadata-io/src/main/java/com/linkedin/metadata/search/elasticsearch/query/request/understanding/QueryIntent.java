package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

/**
 * V2.5 query intent classification. Determines which query strategy to use as the starting tier.
 * Ordered from cheapest (IDENTITY) to most expensive (EXPLORATORY). On zero results, the cascade
 * moves to the next tier.
 */
public enum QueryIntent {
  /** URN, S3/GCS/HDFS path — exact resource lookup. Tier 1. */
  IDENTITY,

  /** Dot-separated FQN (my_db.sales.orders) — navigational lookup. Tier 1. */
  FQN,

  /** Single token or quoted phrase, no dots/spaces — exact name search. Tier 2. */
  EXACT_NAME,

  /** Multi-word descriptive query — broad field search. Tier 3. */
  KEYWORD,

  /** Fallback tier — fuzzy, wildcard, typo recovery. Tier 4. Never classified directly. */
  EXPLORATORY;

  /** Returns the starting tier number (1-4) for cascade ordering. */
  public int startingTier() {
    switch (this) {
      case IDENTITY:
      case FQN:
        return 1;
      case EXACT_NAME:
        return 2;
      case KEYWORD:
        return 3;
      case EXPLORATORY:
        return 4;
    }
    throw new IllegalStateException("Unhandled QueryIntent: " + this);
  }
}
