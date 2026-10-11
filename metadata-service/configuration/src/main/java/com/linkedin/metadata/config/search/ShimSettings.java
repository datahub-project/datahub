package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-cluster search client shim selection ({@code elasticsearch.clusters.<name>.shim}).
 *
 * <p>Each cluster resolves its own engine. A secondary cluster must never inherit the primary's
 * {@code engineType}: the whole point of a second connection is that it may run a different engine
 * version, so an unset shim block auto-detects instead.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ShimSettings {
  /** When false the legacy RestHighLevelClient is used instead of the shim. */
  private Boolean enabled;

  /** AUTO_DETECT, ELASTICSEARCH_8, ELASTICSEARCH_9, OPENSEARCH_2, OPENSEARCH_3. */
  private String engineType;

  /** Takes precedence over {@link #engineType} when true, matching existing behavior. */
  private Boolean autoDetectEngine;

  public boolean isAutoDetectEnabled() {
    // Unset means auto-detect, so a newly added cluster works without pinning a version.
    return autoDetectEngine == null || autoDetectEngine;
  }
}
