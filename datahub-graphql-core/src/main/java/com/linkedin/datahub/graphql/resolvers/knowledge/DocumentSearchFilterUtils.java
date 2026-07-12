package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Utility class for building document search filters with ownership constraints. Shared logic for
 * SearchDocumentsResolver, RelatedDocumentsResolver, and ContextDocumentsResolver.
 */
public class DocumentSearchFilterUtils {

  private DocumentSearchFilterUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Builds a combined filter with ownership constraints that excludes documents explicitly hidden
   * from global context.
   *
   * <p>This is the default for global document searches (sidebar, search popovers). Only documents
   * with showInGlobalContext explicitly set to false are excluded; documents without the
   * documentSettings aspect are shown by default.
   *
   * @param baseCriteria The base user criteria (without state filtering)
   * @param userAndGroupUrns List of URNs for the current user and their groups
   * @return The combined filter excluding showInGlobalContext=false documents
   */
  @Nonnull
  public static Filter buildCombinedFilter(
      @Nonnull List<Criterion> baseCriteria, @Nonnull List<String> userAndGroupUrns) {
    return buildCombinedFilter(baseCriteria, userAndGroupUrns, true);
  }

  /**
   * Builds a combined filter with ownership constraints and optional showInGlobalContext filter.
   *
   * <p>The filter structure is: - With showInGlobalContext: (user-filters AND PUBLISHED AND
   * showInGlobalContext!=false) OR (user-filters AND UNPUBLISHED AND owned-by-user-or-groups AND
   * showInGlobalContext!=false)
   *
   * <p>- Without showInGlobalContext: (user-filters AND PUBLISHED) OR (user-filters AND UNPUBLISHED
   * AND owned-by-user-or-groups)
   *
   * <p>Uses negated EQUAL (field != "false") instead of positive EQUAL (field == "true") so that
   * documents without an explicit documentSettings aspect are included by default. Only documents
   * that explicitly set showInGlobalContext=false are excluded.
   *
   * @param baseCriteria The base user criteria (without state filtering)
   * @param userAndGroupUrns List of URNs for the current user and their groups
   * @param applyShowInGlobalContext Whether to exclude showInGlobalContext=false documents (set to
   *     false for related documents queries where context-only docs should be visible)
   * @return The combined filter
   */
  @Nonnull
  public static Filter buildCombinedFilter(
      @Nonnull List<Criterion> baseCriteria,
      @Nonnull List<String> userAndGroupUrns,
      boolean applyShowInGlobalContext) {

    List<ConjunctiveCriterion> orClauses = new ArrayList<>();

    // Clause 1: Published documents (with user filters)
    List<Criterion> publishedCriteria = new ArrayList<>(baseCriteria);
    publishedCriteria.add(CriterionUtils.buildCriterion("state", Condition.EQUAL, "PUBLISHED"));
    if (applyShowInGlobalContext) {
      publishedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(publishedCriteria)));

    // Clause 2: Unpublished documents owned by user or their groups (with user filters)
    List<Criterion> unpublishedOwnedCriteria = new ArrayList<>(baseCriteria);
    unpublishedOwnedCriteria.add(
        CriterionUtils.buildCriterion("state", Condition.EQUAL, "UNPUBLISHED"));
    unpublishedOwnedCriteria.add(
        CriterionUtils.buildCriterion("owners", Condition.EQUAL, userAndGroupUrns));
    if (applyShowInGlobalContext) {
      unpublishedOwnedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(unpublishedOwnedCriteria)));

    return new Filter().setOr(new ConjunctiveCriterionArray(orClauses));
  }

  /**
   * Builds a negated Criterion (field != value). Documents where the field is absent will pass
   * through (not be excluded).
   */
  private static Criterion buildNegatedCriterion(String field, String value) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(
        new com.linkedin.data.template.StringArray(Collections.singletonList(value)));
    criterion.setNegated(true);
    return criterion;
  }
}
