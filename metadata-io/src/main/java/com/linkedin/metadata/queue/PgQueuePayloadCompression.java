package com.linkedin.metadata.queue;

import java.util.Locale;
import javax.annotation.Nonnull;

/**
 * Application-layer compression codec stored in pgQueue {@code message.payload_compression}
 * (smallint). Values {@code 2+} are reserved for future codecs (e.g. ZSTD); unsupported codes must
 * fail at runtime until implemented.
 *
 * <p>This is separate from PostgreSQL TOAST/storage compression: clients always observe logical
 * {@code bytea} bytes after read; this enum coordinates encode/decode across producers and
 * consumers.
 */
public enum PgQueuePayloadCompression {
  NONE((short) 0),
  SNAPPY((short) 1);

  private final short wireCode;

  PgQueuePayloadCompression(short wireCode) {
    this.wireCode = wireCode;
  }

  public short wireCode() {
    return wireCode;
  }

  @Nonnull
  public static PgQueuePayloadCompression fromWire(int code) {
    if (code == 0) {
      return NONE;
    }
    if (code == 1) {
      return SNAPPY;
    }
    throw new IllegalArgumentException(
        "Unsupported pgQueue payload_compression code: " + code + " (reserved or not implemented)");
  }

  /** Parses Spring/YAML configuration ({@code postgres.pgQueue.producer.payloadCompression}). */
  @Nonnull
  public static PgQueuePayloadCompression fromConfig(@Nonnull String value) {
    String v = value.trim().toUpperCase(Locale.ROOT);
    if (v.isEmpty() || "NONE".equals(v)) {
      return NONE;
    }
    if ("SNAPPY".equals(v)) {
      return SNAPPY;
    }
    throw new IllegalArgumentException(
        "Invalid postgres.pgQueue.producer.payloadCompression: "
            + value
            + " (allowed: NONE, SNAPPY)");
  }
}
