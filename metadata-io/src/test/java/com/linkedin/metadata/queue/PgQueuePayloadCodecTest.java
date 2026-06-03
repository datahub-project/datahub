package com.linkedin.metadata.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class PgQueuePayloadCodecTest {

  @Test
  public void none_roundTrip() {
    byte[] inner = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] stored = PgQueuePayloadCodec.encode(inner, PgQueuePayloadCompression.NONE);
    assertEquals(stored, inner);
    assertEquals(PgQueuePayloadCodec.decode(stored, PgQueuePayloadCompression.NONE), inner);
  }

  @Test
  public void snappy_roundTrip() {
    byte[] inner = new byte[500];
    for (int i = 0; i < inner.length; i++) {
      inner[i] = (byte) (i % 127);
    }
    byte[] stored = PgQueuePayloadCodec.encode(inner, PgQueuePayloadCompression.SNAPPY);
    byte[] back = PgQueuePayloadCodec.decode(stored, PgQueuePayloadCompression.SNAPPY);
    assertEquals(back, inner);
  }

  @Test
  public void fromWire_rejectsUnknownCodec() {
    assertThrows(IllegalArgumentException.class, () -> PgQueuePayloadCompression.fromWire(99));
  }

  @Test
  public void snappy_decodeInvalidBytesThrows() {
    byte[] corrupt = new byte[] {0, 1, 2, 3};
    assertThrows(
        IllegalArgumentException.class,
        () -> PgQueuePayloadCodec.decode(corrupt, PgQueuePayloadCompression.SNAPPY));
  }
}
