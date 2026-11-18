package com.linkedin.metadata.search.utils;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The published-document default the cross-entity GraphQL resolvers add to a search filter: {@code
 * lifecycleStage = PUBLISHED OR (lifecycleStage IS NULL AND state != UNPUBLISHED)}, each clause
 * optionally with {@code showInGlobalContext != false}. Non-document entities pass the second
 * clause. When the caller sets no filter, the resolvers pass exactly this filter, a system default
 * that {@link #isDefault} recognizes.
 */
public final class DocumentDefaultFilter {
  public static final String PUBLISHED_LIFECYCLE_STAGE_URN = "urn:li:lifecycleStageType:PUBLISHED";

  private static final List<Filter> DEFAULTS =
      List.of(build(true, true), build(true, false), build(false, true), build(false, false));

  private DocumentDefaultFilter() {}

  /**
   * @param applyShowInGlobalContext also exclude documents with {@code showInGlobalContext = false}
   * @param canManageDocuments the caller may see unpublished documents, so the legacy clause drops
   *     {@code state != UNPUBLISHED}
   */
  @Nonnull
  public static Filter build(boolean applyShowInGlobalContext, boolean canManageDocuments) {
    final List<Criterion> published = new ArrayList<>(List.of(published()));
    final List<Criterion> legacy = new ArrayList<>(List.of(noLifecycleStage()));
    if (!canManageDocuments) {
      legacy.add(notUnpublished());
    }
    if (applyShowInGlobalContext) {
      published.add(inGlobalContext());
      legacy.add(inGlobalContext());
    }
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(published)),
                new ConjunctiveCriterion().setAnd(new CriterionArray(legacy))));
  }

  /**
   * True when the filter is exactly the default, for one of its flag combinations. The filter is
   * compared whole: a caller filter shaped like part of the default, or the default merged with a
   * caller filter, is not the default.
   */
  public static boolean isDefault(@Nullable Filter filter) {
    return filter != null && DEFAULTS.contains(filter);
  }

  @Nonnull
  private static Criterion published() {
    return equal("lifecycleStage", PUBLISHED_LIFECYCLE_STAGE_URN);
  }

  @Nonnull
  private static Criterion noLifecycleStage() {
    return new Criterion().setField("lifecycleStage").setCondition(Condition.IS_NULL);
  }

  @Nonnull
  private static Criterion notUnpublished() {
    return equal("state", "UNPUBLISHED").setNegated(true);
  }

  @Nonnull
  private static Criterion inGlobalContext() {
    return equal("showInGlobalContext", "false").setNegated(true);
  }

  @Nonnull
  private static Criterion equal(@Nonnull String field, @Nonnull String value) {
    return new Criterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValues(new StringArray(List.of(value)));
  }
}
