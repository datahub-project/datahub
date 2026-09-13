package com.linkedin.metadata.config.search;

import com.linkedin.metadata.utils.ParseUtils;
import java.time.Duration;
import java.util.Locale;
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
   * Bucket size, in {@link #bucketSizeUnit} units; 1d maximum.
   *
   * <p>Zero, negative or over-1d disables canonicalization rather than failing startup. A
   * non-numeric value is different: Spring binding rejects it and GMS does not start. A duration is
   * split across two fields rather than written as one string, so a 5-minute bucket is {@code
   * bucketSize: 5} with {@code bucketSizeUnit: MINUTES} - neither field takes {@code 5m}.
   */
  private int bucketSize;

  /**
   * Unit for {@link #bucketSize}: a {@link java.util.concurrent.TimeUnit} name, e.g. {@code
   * MINUTES}. Omitting it means {@code SECONDS}.
   *
   * <p>The clock has millisecond precision, so {@code MICROSECONDS} and {@code NANOSECONDS} are
   * only accepted when {@link #bucketSize} works out to a whole number of milliseconds. Anything
   * finer disables canonicalization rather than truncating to a bucket the operator did not ask
   * for.
   */
  private String bucketSizeUnit;

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
   * <p>The analytics highlights floor both ends regardless: they compare two periods, so equal
   * widths matter more there than the superset guarantee.
   *
   * <p>An unparseable value disables canonicalization rather than failing startup.
   */
  private String rounding;

  /**
   * Bucket size as a {@link Duration}. Upper-cased here rather than in the shared {@link
   * ParseUtils}, so a Turkish default locale cannot turn {@code minutes} into {@code MİNUTES}.
   */
  public Duration getBucketDuration() {
    return ParseUtils.parseDuration(
        bucketSize, bucketSizeUnit == null ? null : bucketSizeUnit.toUpperCase(Locale.ROOT));
  }
}
