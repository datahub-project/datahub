package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchClauseBudget;
import com.linkedin.search.template.SearchTemplateNormalization;
import java.util.Arrays;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Defaults and safety clamps for {@link SearchTemplateNormalization}. A missing ancestor uses the
 * historical Search V3 limits. Stored values outside those limits are rejected at write; render
 * still clamps them, and the clause budget can keep fewer synonyms or broad-query tokens.
 */
public final class SearchTemplateNormalizationPolicy {
  public static final int DEFAULT_WILDCARD_MINIMUM_LENGTH = 5;
  public static final int MAX_WILDCARD_MINIMUM_LENGTH = 64;
  public static final int DEFAULT_SYNONYM_CAP = 8;
  public static final int MAX_SYNONYM_CAP = 8;
  public static final int DEFAULT_FUZZY_TOKEN_THRESHOLD = 5;
  public static final int MAX_FUZZY_TOKEN_THRESHOLD = 32;
  public static final int DEFAULT_BROAD_QUERY_TOKEN_CAP = 8;
  public static final int MAX_BROAD_QUERY_TOKEN_CAP = 256;
  public static final int DEFAULT_SHORT_FUZZY_EDITS = 1;
  public static final int DEFAULT_MEDIUM_FUZZY_EDITS = 2;
  public static final int MAX_FUZZY_EDITS = 2;
  public static final int MEDIUM_THRESHOLD_GAP = 2;

  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  private SearchTemplateNormalizationPolicy() {}

  public record Resolved(
      int wildcardMinimumLength,
      int synonymCap,
      int fuzzyTokenThreshold,
      int mediumFuzzyTokenThreshold,
      int broadQueryTokenCap,
      int shortFuzzyEdits,
      int mediumFuzzyEdits) {}

  public static boolean empty(@Nullable SearchTemplateNormalization section) {
    if (section == null) {
      return true;
    }
    return !section.hasWildcardMinimumLength()
        && !section.hasSynonymCap()
        && !section.hasFuzzyTokenThreshold()
        && !section.hasMediumFuzzyTokenThreshold()
        && !section.hasBroadQueryTokenCap()
        && !section.hasShortFuzzyEdits()
        && !section.hasMediumFuzzyEdits();
  }

  /**
   * A child replaces only the fields it sets. An explicitly empty section drops the inherited
   * settings.
   */
  @Nullable
  public static SearchTemplateNormalization merge(
      @Nullable SearchTemplateNormalization base, @Nullable SearchTemplateNormalization overlay) {
    if (overlay == null) {
      return base;
    }
    if (empty(overlay)) {
      return null;
    }
    SearchTemplateNormalization merged = copy(base);
    if (overlay.hasWildcardMinimumLength()) {
      merged.setWildcardMinimumLength(overlay.getWildcardMinimumLength());
    }
    if (overlay.hasSynonymCap()) {
      merged.setSynonymCap(overlay.getSynonymCap());
    }
    if (overlay.hasFuzzyTokenThreshold()) {
      merged.setFuzzyTokenThreshold(overlay.getFuzzyTokenThreshold());
    }
    if (overlay.hasMediumFuzzyTokenThreshold()) {
      merged.setMediumFuzzyTokenThreshold(overlay.getMediumFuzzyTokenThreshold());
    }
    if (overlay.hasBroadQueryTokenCap()) {
      merged.setBroadQueryTokenCap(overlay.getBroadQueryTokenCap());
    }
    if (overlay.hasShortFuzzyEdits()) {
      merged.setShortFuzzyEdits(overlay.getShortFuzzyEdits());
    }
    if (overlay.hasMediumFuzzyEdits()) {
      merged.setMediumFuzzyEdits(overlay.getMediumFuzzyEdits());
    }
    return empty(merged) ? null : merged;
  }

  /** Range clamps and defaults. Does not apply the clause budget. */
  @Nonnull
  public static Resolved resolve(@Nullable SearchTemplateNormalization section) {
    int fuzzyTokenThreshold =
        clamp(
            value(section, section != null && section.hasFuzzyTokenThreshold(), fuzzy(section)),
            1,
            MAX_FUZZY_TOKEN_THRESHOLD,
            DEFAULT_FUZZY_TOKEN_THRESHOLD);
    int mediumThreshold =
        section != null
                && section.hasMediumFuzzyTokenThreshold()
                && section.getMediumFuzzyTokenThreshold() != null
            ? clamp(
                section.getMediumFuzzyTokenThreshold(),
                fuzzyTokenThreshold,
                MAX_FUZZY_TOKEN_THRESHOLD + MEDIUM_THRESHOLD_GAP,
                fuzzyTokenThreshold + MEDIUM_THRESHOLD_GAP)
            : Math.min(
                fuzzyTokenThreshold + MEDIUM_THRESHOLD_GAP,
                MAX_FUZZY_TOKEN_THRESHOLD + MEDIUM_THRESHOLD_GAP);
    return new Resolved(
        clamp(
            value(
                section, section != null && section.hasWildcardMinimumLength(), wildcard(section)),
            1,
            MAX_WILDCARD_MINIMUM_LENGTH,
            DEFAULT_WILDCARD_MINIMUM_LENGTH),
        clamp(
            value(section, section != null && section.hasSynonymCap(), synonym(section)),
            0,
            MAX_SYNONYM_CAP,
            DEFAULT_SYNONYM_CAP),
        fuzzyTokenThreshold,
        mediumThreshold,
        clamp(
            value(section, section != null && section.hasBroadQueryTokenCap(), broad(section)),
            1,
            MAX_BROAD_QUERY_TOKEN_CAP,
            DEFAULT_BROAD_QUERY_TOKEN_CAP),
        clamp(
            value(section, section != null && section.hasShortFuzzyEdits(), shortEdits(section)),
            0,
            MAX_FUZZY_EDITS,
            DEFAULT_SHORT_FUZZY_EDITS),
        clamp(
            value(section, section != null && section.hasMediumFuzzyEdits(), mediumEdits(section)),
            0,
            MAX_FUZZY_EDITS,
            DEFAULT_MEDIUM_FUZZY_EDITS));
  }

  /**
   * Lowers the synonym cap and broad-query token cap to what {@code operatorEscaped} can afford.
   * Other settings stay at {@link #resolve}.
   */
  @Nonnull
  public static Resolved budgetClamp(
      @Nonnull Resolved resolved,
      @Nonnull String operatorEscaped,
      @Nonnull SearchClauseBudget.FieldCounts counts) {
    int tokens = tokenCount(operatorEscaped);
    return new Resolved(
        resolved.wildcardMinimumLength(),
        SearchClauseBudget.clampedSynonymCap(resolved.synonymCap(), operatorEscaped, counts),
        resolved.fuzzyTokenThreshold(),
        resolved.mediumFuzzyTokenThreshold(),
        Math.min(resolved.broadQueryTokenCap(), tokens),
        resolved.shortFuzzyEdits(),
        resolved.mediumFuzzyEdits());
  }

  @Nonnull
  public static String firstTokens(@Nullable String text, int cap) {
    if (text == null || text.isBlank() || cap <= 0) {
      return "";
    }
    String trimmed = text.trim();
    String[] words = WHITESPACE.split(trimmed);
    if (words.length <= cap) {
      return trimmed;
    }
    return String.join(" ", Arrays.copyOf(words, cap));
  }

  public static int tokenCount(@Nullable String text) {
    if (text == null || text.isBlank()) {
      return 0;
    }
    return WHITESPACE.split(text.trim()).length;
  }

  @Nonnull
  private static SearchTemplateNormalization copy(@Nullable SearchTemplateNormalization section) {
    if (section == null) {
      return new SearchTemplateNormalization();
    }
    try {
      return section.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Nullable
  private static Integer wildcard(@Nullable SearchTemplateNormalization section) {
    return section == null ? null : section.getWildcardMinimumLength();
  }

  @Nullable
  private static Integer synonym(@Nullable SearchTemplateNormalization section) {
    return section == null ? null : section.getSynonymCap();
  }

  @Nullable
  private static Integer fuzzy(@Nullable SearchTemplateNormalization section) {
    return section == null ? null : section.getFuzzyTokenThreshold();
  }

  @Nullable
  private static Integer broad(@Nullable SearchTemplateNormalization section) {
    return section == null ? null : section.getBroadQueryTokenCap();
  }

  @Nullable
  private static Integer shortEdits(@Nullable SearchTemplateNormalization section) {
    return section == null ? null : section.getShortFuzzyEdits();
  }

  @Nullable
  private static Integer mediumEdits(@Nullable SearchTemplateNormalization section) {
    return section == null ? null : section.getMediumFuzzyEdits();
  }

  @Nullable
  private static Integer value(
      @Nullable SearchTemplateNormalization section, boolean present, @Nullable Integer raw) {
    if (section == null || !present) {
      return null;
    }
    return raw;
  }

  private static int clamp(@Nullable Integer value, int min, int max, int fallback) {
    int resolved = value == null ? fallback : value;
    if (resolved < min) {
      return min;
    }
    return Math.min(resolved, max);
  }
}
