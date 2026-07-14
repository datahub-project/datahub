package com.linkedin.metadata.config;

import lombok.Data;

/** OSS in-memory API usage aggregation and Micrometer export configuration. */
@Data
public class UsageAggregationConfiguration {
  /** Enable in-memory usage aggregation and adaptive flush. */
  private boolean enabled;

  private MicrometerExportConfiguration micrometerExport;

  private FlushConfiguration flush;

  @Data
  public static class MicrometerExportConfiguration {
    /** Export aggregated usage to Micrometer on flush. */
    private boolean enabled;
  }

  @Data
  public static class FlushConfiguration {
    /** Maximum in-memory window duration before flush (seconds). */
    private long maxWindowSeconds;

    /** Cardinality threshold across additive + distinct maps. */
    private int maxCardinality;

    /** Scheduled flush interval (seconds). Zero disables periodic flush. */
    private long scheduledIntervalSeconds;

    /** Publish attempts before re-merging a failed flush batch back into memory. */
    private int retryAttempts = 3;

    /**
     * Initial backoff between publish retries (milliseconds). Doubles each attempt when {@link
     * #retryAttempts} &gt; 1.
     */
    private long retryInitialBackoffMillis = 100;

    /**
     * Calendar grid size for flush windows (seconds). Boundaries are UTC hour/day truncations. Zero
     * disables alignment (process-relative windows).
     */
    private long alignmentPeriodSeconds;
  }
}
