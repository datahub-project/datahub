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
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * The published-document default the cross-entity GraphQL resolvers add to a search filter: {@code
 * lifecycleStage = PUBLISHED OR (lifecycleStage IS NULL AND state != UNPUBLISHED)}, each clause
 * optionally with {@code showInGlobalContext != false}. Non-document entities pass the second
 * clause. It is a system default, not the caller's filter, so code that receives the merged filter
 * recognizes its criteria with {@link #isDefault}.
 */
public final class DocumentDefaultFilter {
  public static final String PUBLISHED_LIFECYCLE_STAGE_URN = "urn:li:lifecycleStageType:PUBLISHED";

  private static final List<Criterion> DEFAULTS =
      List.of(published(), noLifecycleStage(), notUnpublished(), inGlobalContext());

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
   * True when the criterion is one the default adds, compared by field, condition, values, and
   * negation. A caller criterion identical to one of them is treated as the default.
   */
  public static boolean isDefault(@Nonnull Criterion criterion) {
    for (Criterion candidate : DEFAULTS) {
      if (candidate.getField().equals(criterion.getField())
          && candidate.getCondition() == criterion.getCondition()
          && candidate.isNegated().equals(criterion.isNegated())
          && Set.copyOf(candidate.getValues()).equals(Set.copyOf(criterion.getValues()))) {
        return true;
      }
    }
    return false;
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
