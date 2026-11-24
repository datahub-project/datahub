package com.linkedin.metadata.config.search;

import java.util.Set;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An external vector store, named under {@code semanticSearch.stores}, that serves an embedding
 * profile listing that name. This is what the service enforces around the store's adapter.
 */
@Data
@NoArgsConstructor
public class VectorStoreConfiguration {
  /**
   * What the store can do: {@code preFilter} (applies the caller's filter and the system filters
   * inside the vector search) and {@code findById} (returns an entity's query vectors, which
   * similarity needs).
   */
  private Set<String> capabilities = Set.of();

  /** Deadline for one adapter call. The service stops waiting then and cancels the call. */
  private long timeoutMillis = 1_000;

  /** Most neighbors one search requests. */
  private int maxK = 1_000;

  /** How long a page the store served may be reused from the search cache. */
  private long resultTtlMillis = 60_000;
}
