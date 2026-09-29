package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import org.testng.annotations.Test;

public class TraceIdGeneratorTest {

  private static final TraceIdGenerator GENERATOR = new TraceIdGenerator();

  @Test
  public void testGetTimestampMillis_validDataHubTraceId() {
    long now = Instant.now().toEpochMilli();
    String traceId = GENERATOR.generateTraceId(now);

    Long result = TraceIdGenerator.getTimestampMillis(traceId);
    assertNotNull(result);
    // Allow 1ms rounding from micros→millis conversion
    assertTrue(Math.abs(result - now) <= 1, "Expected ~" + now + " but got " + result);
  }

  @Test
  public void testGetTimestampMillis_nullInput() {
    assertNull(TraceIdGenerator.getTimestampMillis(null));
  }

  @Test
  public void testGetTimestampMillis_tooShort() {
    assertNull(TraceIdGenerator.getTimestampMillis("abc123"));
    assertNull(TraceIdGenerator.getTimestampMillis(""));
    assertNull(TraceIdGenerator.getTimestampMillis("0123456789abcde")); // 15 chars
  }

  @Test
  public void testGetTimestampMillis_invalidHex() {
    assertNull(TraceIdGenerator.getTimestampMillis("zzzzzzzzzzzzzzzz0000000000000000"));
  }

  @Test
  public void testGetTimestampMillis_externalW3CTraceId_returnsNull() {
    // W3C trace IDs from external OTel systems have random first 16 hex chars.
    // These parse to implausible timestamps and should return null.
    assertNull(TraceIdGenerator.getTimestampMillis("4bf92f3577b34da6a3ce929d0e0e4736"));
  }

  @Test
  public void testGetTimestampMillis_w3cTraceIdWouldCauseArithmeticOverflow() {
    // The production incident: external W3C trace IDs produce far-future epochs that overflow
    // Duration.ofMillis().toNanos() downstream in MCLKafkaListener.updateMetrics().
    String w3cTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";

    // Verify the raw parse would produce a far-future epoch
    long rawMicros = Long.parseUnsignedLong(w3cTraceId.substring(0, 16), 16);
    long rawMillis = rawMicros / 1000;
    assertTrue(rawMillis > System.currentTimeMillis() * 2);

    // Verify the downstream overflow would occur
    long queueTimeMs = System.currentTimeMillis() - rawMillis;
    try {
      Duration.ofMillis(queueTimeMs).toNanos();
      fail("Expected ArithmeticException");
    } catch (ArithmeticException expected) {
      // This is the crash that getTimestampMillis now prevents
    }

    // Verify the fix: getTimestampMillis returns null, so the overflow path is never reached
    assertNull(TraceIdGenerator.getTimestampMillis(w3cTraceId));
  }

  @Test
  public void testGetTimestampMillis_preDataHubEpoch_returnsNull() {
    // A trace ID encoding epoch 0 (1970-01-01) — before DataHub existed
    String ancientTraceId = GENERATOR.generateTraceId(0L);
    assertNull(TraceIdGenerator.getTimestampMillis(ancientTraceId));
  }

  @Test
  public void testGetTimestampMillis_nearFuture_accepted() {
    // A trace ID 30 seconds in the future should be accepted (clock skew tolerance)
    long nearFuture = Instant.now().toEpochMilli() + 30_000;
    String traceId = GENERATOR.generateTraceId(nearFuture);

    Long result = TraceIdGenerator.getTimestampMillis(traceId);
    assertNotNull(result, "Near-future trace ID should be accepted");
    assertTrue(Math.abs(result - nearFuture) <= 1);
  }

  @Test
  public void testGetTimestampMillis_farFuture_rejected() {
    // A trace ID 48 hours in the future exceeds the 24-hour clock skew tolerance
    long farFuture = Instant.now().toEpochMilli() + Duration.ofHours(48).toMillis();
    String traceId = GENERATOR.generateTraceId(farFuture);

    assertNull(
        TraceIdGenerator.getTimestampMillis(traceId), "Far-future trace ID should be rejected");
  }

  @Test
  public void testGetTimestampMillis_roundTrip() {
    // Generate a trace ID and verify the timestamp survives the round trip
    long original = Instant.now().toEpochMilli();
    String traceId = GENERATOR.generateTraceId(original);

    Long extracted = TraceIdGenerator.getTimestampMillis(traceId);
    assertNotNull(extracted);
    assertEquals(extracted.longValue(), original);
  }
}
