package com.linkedin.metadata.graph.postgres;

import javax.annotation.Nonnull;

/**
 * Stable XXHash64 for SqlSetup {@code xxhash64_id} columns (vertex primary keys). Matches reference
 * XXHash64 (seed 0) over UTF-8 bytes.
 */
public final class UrnFingerprint64 {

  private UrnFingerprint64() {}

  public static long ofUtf8String(@Nonnull String s) {
    return XxHash64.hashUtf8(s);
  }
}
