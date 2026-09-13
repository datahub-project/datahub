package com.linkedin.metadata.utils.elasticsearch.canonicalization;

import java.util.Locale;
import javax.annotation.Nullable;

/** How a time boundary is moved onto a bucket boundary. */
public enum TimeRoundingMode {
  /**
   * Floor lower bounds ({@code gt}, {@code gte}), ceil upper bounds ({@code lt}, {@code lte}). The
   * canonical window is always a superset of the requested window, so no document that the exact
   * query would have matched is excluded. The cost is that a trailing window may report up to one
   * extra bucket at its leading edge.
   */
  EXPAND,

  /**
   * Floor both bounds. Produces the tidiest windows (both ends land on the same wall-clock
   * boundary), but the upper bound moves backwards, hiding up to one bucket of the most recent
   * data.
   */
  SHRINK;

  public static TimeRoundingMode fromString(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return EXPAND;
    }
    try {
      return TimeRoundingMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unsupported time rounding mode '" + value + "'. Expected one of: EXPAND, SHRINK.");
    }
  }
}
