package com.linkedin.metadata.config.graphql;

import lombok.Data;

/**
 * Configuration for GraphQL query/response shape logging.
 *
 * <p>When enabled, emits structured JSON log entries (at INFO level via the {@code
 * com.datahub.graphql.shape} logger) whenever at least one threshold is crossed. Cheap Micrometer
 * shape metrics are emitted regardless of thresholds.
 */
@Data
public class GraphQLShapeLoggingConfiguration {
  /** Master switch. When false, no shape analysis or structured logging is performed. */
  private boolean enabled = false;

  /**
   * Minimum number of GraphQL field selections in the request to trigger shape logging.
   *
   * <p>Field count is a proxy for query complexity.
   */
  private int fieldCountThreshold = 100;

  /**
   * Minimum request duration in milliseconds to trigger shape logging.
   *
   * <p>Slow-query threshold.
   */
  private long durationThresholdMs = 3000;

  /**
   * Estimated minimum response size in bytes to trigger shape logging.
   *
   * <p>Size is approximated from the response field count (fieldCount * 50 bytes).
   *
   * <p><strong>NOTE:</strong> This threshold is only evaluated AFTER at least one other threshold
   * (fieldCount, duration, or errorCount) has already been crossed. Response shape analysis is
   * deferred until another threshold triggers to avoid expensive computation on the hot path.
   * Therefore, setting only this threshold (with high values for others) will NOT trigger shape
   * logs. Combine with at least one other threshold.
   */
  private long responseSizeThresholdBytes = 1_048_576; // 1 MiB

  /**
   * Minimum number of GraphQL errors in the response to trigger shape logging.
   *
   * <p>Set to 1 to log any error responses.
   */
  private int errorCountThreshold = 1;
}
