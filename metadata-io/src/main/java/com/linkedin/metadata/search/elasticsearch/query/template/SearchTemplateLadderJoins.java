package com.linkedin.metadata.search.elasticsearch.query.template;

import java.util.Locale;
import javax.annotation.Nonnull;

/** Parallel joins. {@code any} returns the first member that stops. {@code allEmpty} waits. */
public enum SearchTemplateLadderJoins {
  ANY,
  ALL_EMPTY,
  MERGE;

  @Nonnull
  public static SearchTemplateLadderJoins parse(@Nonnull String join) {
    return switch (join.trim().toLowerCase(Locale.ROOT)) {
      case "any" -> ANY;
      case "allempty", "all_empty" -> ALL_EMPTY;
      case "merge" -> MERGE;
      default -> throw new SearchTemplateException("Search template join is not valid: " + join);
    };
  }
}
