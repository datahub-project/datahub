package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import javax.annotation.Nonnull;

/** How one field matched. Reported on a hit only when the caller asks for an explanation. */
public record MatchEvidence(
    @Nonnull String field,
    @Nonnull String value,
    @Nonnull String subfield,
    @Nonnull String mode,
    @Nonnull String query) {
  public static final String EXACT = "exact";
  public static final String PREFIX = "prefix";
  public static final String WORD = "word";
  public static final String CONTAINS = "contains";
  public static final String NAME_AND_VALUE = "nameAndValue";
}
