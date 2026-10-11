package com.linkedin.metadata.queue;

import java.io.IOException;
import javax.annotation.Nonnull;
import org.xerial.snappy.Snappy;

/**
 * Encode/decode pgQueue {@code payload} bytes using {@link PgQueuePayloadCompression}.
 *
 * <p>This is explicit application-layer compression recorded in {@code
 * message.payload_compression}. PostgreSQL may still apply TOAST storage compression for large
 * {@code bytea} values; that is transparent on read and does not replace per-row codec coordination
 * between producers and consumers.
 */
public final class PgQueuePayloadCodec {

  private PgQueuePayloadCodec() {}

  /** Compress inner bytes (e.g. Confluent Avro wire format) for storage. */
  @Nonnull
  public static byte[] encode(@Nonnull byte[] inner, @Nonnull PgQueuePayloadCompression mode) {
    switch (mode) {
      case NONE:
        return inner;
      case SNAPPY:
        try {
          return Snappy.compress(inner);
        } catch (IOException e) {
          throw new IllegalArgumentException("Snappy compress failed for pgQueue payload", e);
        }
      default:
        throw new IllegalStateException("Unhandled compression mode: " + mode);
    }
  }

  /** Decompress stored bytes to inner format expected by Avro deserializers. */
  @Nonnull
  public static byte[] decode(@Nonnull byte[] stored, @Nonnull PgQueuePayloadCompression mode) {
    switch (mode) {
      case NONE:
        return stored;
      case SNAPPY:
        try {
          return Snappy.uncompress(stored);
        } catch (IOException e) {
          throw new IllegalArgumentException("Snappy decompress failed for pgQueue payload", e);
        }
      default:
        throw new IllegalStateException("Unhandled compression mode: " + mode);
    }
  }
}
