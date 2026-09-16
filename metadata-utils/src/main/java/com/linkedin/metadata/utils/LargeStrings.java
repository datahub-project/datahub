package com.linkedin.metadata.utils;

import com.linkedin.common.CompressionType;
import com.linkedin.common.LargeString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;

/**
 * Encode/decode helpers for {@link LargeString}. The logical value is always the decompressed UTF-8
 * text; the stored {@code blob} is either that raw text (compression = NONE) or {@code
 * base64(gzip(utf8(text)))} (compression = GZIP). Base64 keeps the blob JSON-safe for JSON-stored
 * aspects.
 */
public final class LargeStrings {

  /**
   * Text at or below this many UTF-8 bytes is stored uncompressed (NONE) so small contracts stay
   * human-readable/greppable in the raw aspect. Must match the Python helper's default threshold.
   */
  public static final long DEFAULT_THRESHOLD_BYTES = 262_144L; // 256 KiB

  private LargeStrings() {}

  /** Encode with the default threshold. */
  @Nonnull
  public static LargeString encode(@Nonnull final String text) {
    return encode(text, DEFAULT_THRESHOLD_BYTES);
  }

  /**
   * Encode {@code text} into a {@link LargeString}. Compresses (GZIP) only when the UTF-8 byte
   * length exceeds {@code thresholdBytes}; otherwise stores the raw text (NONE). {@code
   * uncompressedSize} is always set to the original UTF-8 byte length.
   */
  @Nonnull
  public static LargeString encode(@Nonnull final String text, final long thresholdBytes) {
    final byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
    final LargeString result = new LargeString();
    result.setUncompressedSize((long) utf8.length);

    if (utf8.length <= thresholdBytes) {
      result.setCompression(CompressionType.NONE);
      result.setBlob(text);
      return result;
    }

    result.setCompression(CompressionType.GZIP);
    result.setBlob(Base64.getEncoder().encodeToString(gzip(utf8)));
    return result;
  }

  /**
   * Decode a {@link LargeString} back to its logical (decompressed) UTF-8 text.
   *
   * @throws IllegalArgumentException if the declared {@link CompressionType} is unknown or the blob
   *     cannot be decoded under it.
   */
  @Nonnull
  public static String decode(@Nonnull final LargeString value) {
    final CompressionType compression = value.getCompression();
    final String blob = value.getBlob();
    switch (compression) {
      case NONE:
        return blob;
      case GZIP:
        final byte[] compressed;
        try {
          compressed = Base64.getDecoder().decode(blob);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "LargeString declares GZIP compression but blob is not valid base64", e);
        }
        return new String(gunzip(compressed), StandardCharsets.UTF_8);
      default:
        throw new IllegalArgumentException("Unknown LargeString CompressionType: " + compression);
    }
  }

  private static byte[] gzip(@Nonnull final byte[] data) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, data.length / 4));
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(data);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to gzip LargeString blob", e);
    }
    return out.toByteArray();
  }

  private static byte[] gunzip(@Nonnull final byte[] data) {
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(data))) {
      final ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, data.length * 4));
      final byte[] buf = new byte[8192];
      int read;
      while ((read = gzip.read(buf)) != -1) {
        out.write(buf, 0, read);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "LargeString declares GZIP compression but blob is not valid gzip data", e);
    }
  }
}
