package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for throttling timeseries aspect writes to Elasticsearch indices. A single refresh
 * period controls the rate limit; independent enable flags gate which write paths (entity search
 * index and/or timeseries index) are suppressed when throttled. The cache TTL is aligned with the
 * refresh period automatically.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class TimeseriesWriteThrottleConfiguration {

  @Builder.Default private IndexThrottleConfig entityIndex = new IndexThrottleConfig();
  @Builder.Default private IndexThrottleConfig timeseriesIndex = new IndexThrottleConfig();

  /** Log-only mode: tracks what would be throttled without suppressing writes. */
  @Builder.Default private IndexThrottleConfig observe = new IndexThrottleConfig();

  @Builder.Default private long refreshPeriodSeconds = 3600;

  /**
   * JSON override for per-entity, per-aspect refresh periods. Format: {@code {"entityType":
   * {"aspectName": seconds}}}. Aspects not listed fall back to {@link #refreshPeriodSeconds}.
   */
  @Builder.Default private String refreshOverrides = "{}";

  @Builder.Default private int maxCacheUrns = 10_000;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class IndexThrottleConfig {
    @Builder.Default private boolean enabled = false;
  }
}
