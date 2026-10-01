package com.linkedin.metadata.utils.elasticsearch;

import io.opentelemetry.api.trace.Tracer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Settings for bulk-write attribution, passed to {@link SearchClientShim#configureBulkTelemetry}.
 * Immutable; {@link #DISABLED} is the all-off instance and the default everywhere.
 *
 * <ul>
 *   <li>{@code batchSpans}: emit one {@code index bulk} span per flushed batch, using {@code
 *       tracer}. Ignored when {@code tracer} is null.
 *   <li>{@code opaqueId}: send the batch id to the store as {@code X-Opaque-Id} (OpenSearch client
 *       only; the Elasticsearch 8 bulk ingester owns its HTTP calls).
 *   <li>{@code serviceName}: the service named in that header; the implementation falls back to a
 *       default when null or blank.
 * </ul>
 */
@Value
@Builder(toBuilder = true)
public class BulkTelemetryConfig {

  /** Attribution off: no span, no header. */
  public static final BulkTelemetryConfig DISABLED = BulkTelemetryConfig.builder().build();

  @Nullable Tracer tracer;
  boolean batchSpans;
  boolean opaqueId;
  @Nullable String serviceName;

  /** Spans will be produced: {@code batchSpans} is set and a tracer was supplied. */
  public boolean spansEnabled() {
    return batchSpans && tracer != null;
  }

  /** Anything at all is on. */
  public boolean isEnabled() {
    return spansEnabled() || opaqueId;
  }

  /** Convenience factory mirroring the four settings. */
  @Nonnull
  public static BulkTelemetryConfig of(
      @Nullable Tracer tracer, boolean batchSpans, boolean opaqueId, @Nullable String serviceName) {
    return BulkTelemetryConfig.builder()
        .tracer(tracer)
        .batchSpans(batchSpans)
        .opaqueId(opaqueId)
        .serviceName(serviceName)
        .build();
  }
}
