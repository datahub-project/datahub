package com.linkedin.metadata.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class PgQueuePayloadCompressionTest {

  @Test
  public void testFromConfigNone() {
    assertEquals(PgQueuePayloadCompression.fromConfig("NONE"), PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testFromConfigSnappy() {
    assertEquals(PgQueuePayloadCompression.fromConfig("SNAPPY"), PgQueuePayloadCompression.SNAPPY);
  }

  @Test
  public void testFromConfigCaseInsensitive() {
    assertEquals(PgQueuePayloadCompression.fromConfig("snappy"), PgQueuePayloadCompression.SNAPPY);
    assertEquals(PgQueuePayloadCompression.fromConfig("Snappy"), PgQueuePayloadCompression.SNAPPY);
    assertEquals(PgQueuePayloadCompression.fromConfig("none"), PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testFromConfigTrimmed() {
    assertEquals(
        PgQueuePayloadCompression.fromConfig("  SNAPPY  "), PgQueuePayloadCompression.SNAPPY);
    assertEquals(PgQueuePayloadCompression.fromConfig("  NONE  "), PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testFromConfigEmptyDefaultsToNone() {
    assertEquals(PgQueuePayloadCompression.fromConfig(""), PgQueuePayloadCompression.NONE);
    assertEquals(PgQueuePayloadCompression.fromConfig("  "), PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testFromConfigInvalidThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> PgQueuePayloadCompression.fromConfig("ZSTD"));
    assertThrows(
        IllegalArgumentException.class, () -> PgQueuePayloadCompression.fromConfig("invalid"));
  }

  @Test
  public void testFromWireNone() {
    assertEquals(PgQueuePayloadCompression.fromWire(0), PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testFromWireSnappy() {
    assertEquals(PgQueuePayloadCompression.fromWire(1), PgQueuePayloadCompression.SNAPPY);
  }

  @Test
  public void testFromWireUnknownThrows() {
    assertThrows(IllegalArgumentException.class, () -> PgQueuePayloadCompression.fromWire(99));
  }

  @Test
  public void testWireCodeRoundTrip() {
    for (PgQueuePayloadCompression c : PgQueuePayloadCompression.values()) {
      assertEquals(PgQueuePayloadCompression.fromWire(c.wireCode()), c);
    }
  }
}
