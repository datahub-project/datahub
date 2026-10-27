package com.linkedin.metadata.graph.lineage.core;

/** Extracts the entity type and data platform from a URN's bytes without parsing it. */
final class UrnParts {

  private static final byte[] PREFIX = "urn:li:".getBytes();
  private static final byte[] PLATFORM = "urn:li:dataPlatform:".getBytes();

  private UrnParts() {}

  /** Entity type, e.g. {@code dataset} for {@code urn:li:dataset:(...)}; "" if not a urn:li URN. */
  static String entityType(byte[] b, int off, int len) {
    if (!startsWith(b, off, len, PREFIX, 0)) {
      return "";
    }
    int start = off + PREFIX.length;
    int end = start;
    while (end < off + len && b[end] != ':') {
      end++;
    }
    return new String(b, start, end - start);
  }

  /** Platform name from the first embedded {@code urn:li:dataPlatform:} URN, or null. */
  static String platform(byte[] b, int off, int len) {
    int limit = off + len - PLATFORM.length;
    for (int i = off + PREFIX.length; i <= limit; i++) {
      if (b[i] == 'u' && startsWith(b, i, off + len - i, PLATFORM, 0)) {
        int start = i + PLATFORM.length;
        int end = start;
        while (end < off + len && b[end] != ',' && b[end] != ')') {
          end++;
        }
        return new String(b, start, end - start);
      }
    }
    return null;
  }

  private static boolean startsWith(byte[] b, int off, int len, byte[] prefix, int from) {
    if (len < prefix.length) {
      return false;
    }
    for (int i = from; i < prefix.length; i++) {
      if (b[off + i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }
}
