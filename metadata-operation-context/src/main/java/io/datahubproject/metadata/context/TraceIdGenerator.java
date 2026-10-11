package io.datahubproject.metadata.context;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.sdk.trace.IdGenerator;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

public class TraceIdGenerator implements IdGenerator {
  private final IdGenerator defaultGenerator;

  // DataHub's tracing system didn't exist before 2020
  private static final long MIN_PLAUSIBLE_EPOCH_MS = 1_577_836_800_000L; // 2020-01-01 UTC
  private static final long MAX_CLOCK_SKEW_MS = 86_400_000L; // 24 hours

  public TraceIdGenerator() {
    this.defaultGenerator = IdGenerator.random();
  }

  @VisibleForTesting
  public String generateTraceId(long epochMillis) {
    // First 8 bytes (16 hex chars) as timestamp in micros
    long timestampMicros = epochMillis * 1000;
    // Last 8 bytes as random to ensure uniqueness
    long randomBits = ThreadLocalRandom.current().nextLong();

    return String.format("%016x%016x", timestampMicros, randomBits);
  }

  @Override
  public String generateTraceId() {
    return generateTraceId(Instant.now().toEpochMilli());
  }

  @Override
  public String generateSpanId() {
    // Use default random generation for span IDs
    return defaultGenerator.generateSpanId();
  }

  /**
   * Extracts the epoch millis encoded in a DataHub trace ID. Returns null if the trace ID is
   * malformed, too short, or encodes an implausible timestamp (e.g., external W3C trace IDs from
   * OTel service meshes where the first 16 hex chars are random, not epoch micros).
   */
  @Nullable
  public static Long getTimestampMillis(@Nullable String traceId) {
    if (traceId == null || traceId.length() < 16) {
      return null;
    }
    try {
      long epochMillis = Long.parseUnsignedLong(traceId.substring(0, 16), 16) / 1000;
      if (epochMillis >= MIN_PLAUSIBLE_EPOCH_MS
          && epochMillis <= System.currentTimeMillis() + MAX_CLOCK_SKEW_MS) {
        return epochMillis;
      }
    } catch (NumberFormatException e) {
      // Not valid hex — e.g., non-hex characters in trace ID
    }
    return null;
  }
}
