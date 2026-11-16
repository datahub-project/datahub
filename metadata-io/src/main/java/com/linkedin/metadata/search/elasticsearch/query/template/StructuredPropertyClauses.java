package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyQuery;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Inheritance and enablement for {@link StructuredPropertyQuery}. */
public final class StructuredPropertyClauses {
  private StructuredPropertyClauses() {}

  public static boolean empty(@Nullable StructuredPropertyQuery section) {
    if (section == null) {
      return true;
    }
    return !section.hasAnalyzedValue() && !section.hasExactValue() && !section.hasNameAndValue();
  }

  /**
   * A child replaces only the clauses it sets. An explicitly empty section drops the inherited
   * clauses.
   */
  @Nullable
  public static StructuredPropertyQuery merge(
      @Nullable StructuredPropertyQuery base, @Nullable StructuredPropertyQuery overlay) {
    if (overlay == null) {
      return base;
    }
    if (empty(overlay)) {
      return null;
    }
    StructuredPropertyQuery merged = copy(base);
    if (overlay.hasAnalyzedValue()) {
      merged.setAnalyzedValue(overlay.getAnalyzedValue());
    }
    if (overlay.hasExactValue()) {
      merged.setExactValue(overlay.getExactValue());
    }
    if (overlay.hasNameAndValue()) {
      merged.setNameAndValue(overlay.getNameAndValue());
    }
    return empty(merged) ? null : merged;
  }

  /** A clause record runs unless it sets {@code enabled} false. */
  public static boolean runs(@Nullable StructuredPropertyClause clause) {
    if (clause == null) {
      return false;
    }
    return !clause.hasEnabled() || !Boolean.FALSE.equals(clause.isEnabled());
  }

  /** Positive weight, or 1 when the clause does not set one. */
  public static float weight(@Nullable StructuredPropertyClause clause) {
    if (clause == null || !clause.hasWeight() || clause.getWeight() == null) {
      return 1f;
    }
    float value = clause.getWeight();
    if (!Float.isFinite(value) || value <= 0f) {
      return 1f;
    }
    return value;
  }

  @Nonnull
  private static StructuredPropertyQuery copy(@Nullable StructuredPropertyQuery section) {
    if (section == null) {
      return new StructuredPropertyQuery();
    }
    try {
      return section.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }
}
