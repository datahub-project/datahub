package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Root configuration for query canonicalization.
 *
 * <p>Canonicalization rewrites semantically equivalent queries into one canonical form so that
 * Elasticsearch/OpenSearch has the opportunity to reuse cached work. Time canonicalization is the
 * only strategy today; the structure allows further strategies (field normalization, stable clause
 * ordering, ...) to be added without changing call sites.
 *
 * <p>{@code enabled} is a hard off switch for every strategy. Defaults live in {@code
 * application.yaml}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class QueryCanonicalizationConfiguration {

  /** Master switch. When false, no canonicalizer runs and queries are passed through unchanged. */
  private boolean enabled;

  /** Time-based canonicalization settings. */
  private TimeCanonicalizationConfiguration time;
}
