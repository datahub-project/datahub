package com.linkedin.metadata.recommendation.candidatesource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 * Normalizes search facet bucket keys that should be URNs. Some documents incorrectly store a
 * JSON-encoded single-element string array (e.g. {@code ["urn:li:tag:Legacy"]}) as the facet value
 * instead of the bare URN string.
 */
public final class FacetAggregationUrnKeys {

  private static final Pattern JSON_ARRAY_SINGLE_QUOTED_URN =
      Pattern.compile("^\\[\\s*\"(urn:li:[^\"]+)\"\\s*\\]$");

  private FacetAggregationUrnKeys() {}

  /**
   * If {@code rawKey} looks like {@code ["urn:li:..."]}, returns the inner URN; otherwise returns
   * {@code rawKey} trimmed (or empty string if null).
   */
  @Nonnull
  public static String coerceFacetKeyToUrnString(@Nonnull String rawKey) {
    String trimmed = rawKey.trim();
    Matcher m = JSON_ARRAY_SINGLE_QUOTED_URN.matcher(trimmed);
    if (m.matches()) {
      return m.group(1);
    }
    return trimmed;
  }
}
