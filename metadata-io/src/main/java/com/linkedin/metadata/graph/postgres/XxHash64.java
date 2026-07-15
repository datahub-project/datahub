package com.linkedin.metadata.graph.postgres;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * XXHash64 (xxHash r39, 64-bit variant) via JNI through lz4-java's {@link
 * XXHashFactory#nativeInstance()}. There is no pure-Java fallback: if the native library cannot
 * load, class initialization fails.
 *
 * <p>Stable BIGINT identifiers match common PostgreSQL xxhash extensions and CLI {@code xxhsum}.
 * Seed 0 matches reference implementations.
 *
 * <p>Algorithm reference: https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md
 */
public final class XxHash64 {

  private static final XXHash64 DELEGATE = XXHashFactory.nativeInstance().hash64();

  private XxHash64() {}

  /** XXHash64 with seed 0 over UTF-8 bytes of {@code s}. */
  public static long hashUtf8(@Nonnull String s) {
    return hashBytes(s.getBytes(StandardCharsets.UTF_8), 0L);
  }

  /** XXHash64 over bytes with the given seed (use 0 for canonical ids). */
  public static long hashBytes(@Nonnull byte[] input, long seed) {
    return hashBytes(input, 0, input.length, seed);
  }

  public static long hashBytes(@Nonnull byte[] input, int offset, int length, long seed) {
    int end = offset + length;
    if (offset < 0 || length < 0 || end > input.length) {
      throw new IllegalArgumentException("range out of bounds");
    }
    return DELEGATE.hash(input, offset, length, seed);
  }
}
