package com.linkedin.metadata.search.rescore;

/** Types of signals that can be extracted from search results. */
public enum SignalType {
  /** The _score from Elasticsearch (BM25 + boosts) */
  SCORE,
  /** Numeric field value (viewCount, usageCount, etc.) */
  NUMERIC,
  /** Boolean field value (hasDescription, deprecated, etc.) */
  BOOLEAN,
  /** Timestamp field for recency calculations */
  TIMESTAMP,
  /** Index name for entity type detection (glossaryterm, domain, etc.) */
  INDEX_NAME
}
