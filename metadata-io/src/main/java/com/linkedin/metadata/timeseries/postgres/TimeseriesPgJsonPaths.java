package com.linkedin.metadata.timeseries.postgres;

import javax.annotation.Nonnull;

/** Utilities for resolving Elasticsearch-shaped field names to PostgreSQL {@code jsonb} paths. */
public final class TimeseriesPgJsonPaths {

  private TimeseriesPgJsonPaths() {}

  /** Strip trailing {@code .keyword} used by Elasticsearch for exact match on text fields. */
  @Nonnull
  public static String stripKeywordSuffix(@Nonnull String field) {
    String f = field;
    if (f.endsWith(".keyword")) {
      return f.substring(0, f.length() - ".keyword".length());
    }
    return f;
  }

  /**
   * Split a dotted field path into JSON segments for {@code document #>> ARRAY[...]} or {@code
   * document @> ...}.
   */
  @Nonnull
  public static String[] pathSegments(@Nonnull String dottedField) {
    return dottedField.split("\\.");
  }
}
