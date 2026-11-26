package com.linkedin.metadata.service;

/**
 * How a service-layer write should be applied to the search index.
 *
 * <p>{@link #SYNC} marks the write's proposals so that GMS updates the search/graph indices
 * synchronously in the request path and the MAE consumer skips them. Interactive callers (GraphQL
 * resolvers backing the UI) should use this so users read their own writes.
 *
 * <p>{@link #ASYNC} leaves indexing to the MAE consumer: eventually consistent, but at-least-once
 * (the change log is replayed from Kafka on consumer failure). Programmatic/bulk callers should
 * prefer this.
 *
 * <p><b>Pick one mode per entity.</b> The two modes are indexed by different processes on
 * independent bulk-flush timers, so interleaving SYNC and ASYNC writes to the same entity can apply
 * out of order and leave the index with stale values until the next write to that aspect.
 */
public enum SearchIndexMode {
  /** Index synchronously in GMS; the MAE consumer skips the event. */
  SYNC,
  /** Leave indexing to the MAE consumer. */
  ASYNC
}
