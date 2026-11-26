package com.linkedin.metadata.graph;

import javax.annotation.Nullable;

/** Combines the reasons of partial lineage results. */
public final class LineagePartialReasons {

  private LineagePartialReasons() {}

  /**
   * The reason for a result that merges two others. An incomplete fetch outranks the limit: walking
   * again can still return more, which is what callers deciding how long to keep a result care
   * about.
   */
  @Nullable
  public static LineagePartialReason combine(
      @Nullable LineagePartialReason first, @Nullable LineagePartialReason second) {
    if (first == LineagePartialReason.INCOMPLETE_FETCH
        || second == LineagePartialReason.INCOMPLETE_FETCH) {
      return LineagePartialReason.INCOMPLETE_FETCH;
    }
    return first != null ? first : second;
  }
}
