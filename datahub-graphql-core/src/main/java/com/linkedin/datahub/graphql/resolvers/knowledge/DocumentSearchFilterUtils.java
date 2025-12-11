package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import java.util.ArrayList;
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
   * Builds a combined filter with ownership constraints and showInGlobalContext=true requirement.
   *
   * <p>This is the default for global document searches (sidebar, search popovers). Documents with
   * showInGlobalContext=false are context-only and should not appear in global searches.
   *
   * @param baseCriteria The base user criteria (without state filtering)
   * @param userAndGroupUrns List of URNs for the current user and their groups
   * @return The combined filter with showInGlobalContext=true requirement
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
   * NOT-DRAFT AND showInGlobalContext=true) OR (user-filters AND UNPUBLISHED AND
   * owned-by-user-or-groups AND NOT-DRAFT AND showInGlobalContext=true)
   *
   * <p>- Without showInGlobalContext: (user-filters AND PUBLISHED AND NOT-DRAFT) OR (user-filters
   * AND UNPUBLISHED AND owned-by-user-or-groups AND NOT-DRAFT)
   *
   * <p>Drafts (documents with draftOf field set) are always excluded from search results. They
   * should only be accessed directly by URN or through the DocumentDrafts resolver.
   *
   * @param baseCriteria The base user criteria (without state filtering)
   * @param userAndGroupUrns List of URNs for the current user and their groups
   * @param applyShowInGlobalContext Whether to require showInGlobalContext=true (set to false for
   *     related documents queries where context-only docs should be visible)
   * @return The combined filter
   */
  @Nonnull
  public static Filter buildCombinedFilter(
      @Nonnull List<Criterion> baseCriteria,
      @Nonnull List<String> userAndGroupUrns,
      boolean applyShowInGlobalContext) {

    List<ConjunctiveCriterion> orClauses = new ArrayList<>();

    // Create criterion to exclude drafts (draftOf field must be null/not set)
    Criterion notDraftCriterion = new Criterion();
    notDraftCriterion.setField("draftOf");
    notDraftCriterion.setCondition(Condition.IS_NULL);

    // Clause 1: Published documents (with user filters, excluding drafts)
    List<Criterion> publishedCriteria = new ArrayList<>(baseCriteria);
    publishedCriteria.add(CriterionUtils.buildCriterion("state", Condition.EQUAL, "PUBLISHED"));
    publishedCriteria.add(notDraftCriterion);
    if (applyShowInGlobalContext) {
      publishedCriteria.add(
          CriterionUtils.buildCriterion("showInGlobalContext", Condition.EQUAL, "true"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(publishedCriteria)));

    // Clause 2: Unpublished documents owned by user or their groups (with user filters, excluding
    // drafts)
    List<Criterion> unpublishedOwnedCriteria = new ArrayList<>(baseCriteria);
    unpublishedOwnedCriteria.add(
        CriterionUtils.buildCriterion("state", Condition.EQUAL, "UNPUBLISHED"));
    unpublishedOwnedCriteria.add(
        CriterionUtils.buildCriterion("owners", Condition.EQUAL, userAndGroupUrns));
    // Create new criterion instance for the second clause to avoid sharing mutable state
    Criterion notDraftCriterion2 = new Criterion();
    notDraftCriterion2.setField("draftOf");
    notDraftCriterion2.setCondition(Condition.IS_NULL);
    unpublishedOwnedCriteria.add(notDraftCriterion2);
    if (applyShowInGlobalContext) {
      unpublishedOwnedCriteria.add(
          CriterionUtils.buildCriterion("showInGlobalContext", Condition.EQUAL, "true"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(unpublishedOwnedCriteria)));

    return new Filter().setOr(new ConjunctiveCriterionArray(orClauses));
  }
}
