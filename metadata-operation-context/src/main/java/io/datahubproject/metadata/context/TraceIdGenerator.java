/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.metadata.context;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.sdk.trace.IdGenerator;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class TraceIdGenerator implements IdGenerator {
  private final IdGenerator defaultGenerator;

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

  // Utility method to extract timestamp
  private static long getTimestampMicros(String traceId) {
    if (traceId == null || traceId.length() < 16) {
      throw new IllegalArgumentException("Invalid trace ID format");
    }
    return Long.parseUnsignedLong(traceId.substring(0, 16), 16);
  }

  // Convert to milliseconds for easier comparison
  public static long getTimestampMillis(String traceId) {
    return getTimestampMicros(traceId) / 1000;
  }
}
