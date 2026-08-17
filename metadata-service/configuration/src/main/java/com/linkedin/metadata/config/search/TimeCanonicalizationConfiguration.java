package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for time-based query canonicalization.
 *
 * <p>Time canonicalization rounds the time boundaries of a query to a fixed bucket so that requests
 * arriving within the same bucket produce a byte-identical Elasticsearch/OpenSearch query.
 * Identical queries are a precondition for the shard request cache to be reused; they are not a
 * guarantee of a cache hit, and the bucket size is NOT a cache TTL.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class TimeCanonicalizationConfiguration {

  /**
   * Enables the canonical reference clock. When enabled, the code paths that derive a query window
   * from the wall clock (usage stats, operations stats, dashboard stats, DataHub usage events)
   * resolve "now" to a bucket boundary instead of the current millisecond.
   */
  private boolean enabled;

  /**
   * Bucket size as a duration string, e.g. {@code 1m}, {@code 5m}, {@code 15m}, {@code 30m}, {@code
   * 1h}. Any {@code <number><unit>} value is accepted with units ms/s/m/h/d, up to a maximum of 1d.
   * This is an arbitrary duration, not an enum - the listed values are conventional, not
   * exhaustive.
   *
   * <p>An unparseable value disables canonicalization rather than failing startup.
   */
  private String bucketSize;

  /**
   * Timezone whose local midnight anchors the bucket boundaries. Defaults to {@code UTC}, which
   * anchors buckets at the epoch. Only relevant for operators who want buckets aligned to local
   * wall-clock time; all indexed timestamps are epoch millis regardless.
   */
  private String timezone;

  /**
   * Rounding strategy. {@code EXPAND} floors lower bounds and ceils upper bounds so the canonical
   * window is always a superset of the requested window (no data is hidden). {@code SHRINK} floors
   * both ends, which produces tidier windows but hides up to one bucket of the most recent data.
   *
   * <p>An unparseable value disables canonicalization rather than failing startup.
   */
  private String rounding;
}
