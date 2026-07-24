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
 *
 * <p>Visibility is determined by two fields with a priority rule:
 *
 * <ul>
 *   <li><b>lifecycleStage</b> (on the {@code status} aspect) — preferred, new field
 *   <li><b>state</b> (on {@code documentInfo.status}) — legacy, used as fallback
 * </ul>
 *
 * When {@code lifecycleStage} is present it takes precedence: PUBLISHED URN → visible to all,
 * UNPUBLISHED/IN_REVIEW/etc URN → visible only to owners, DRAFT URN → never visible (even to
 * owners). When absent, the legacy {@code state} field is used with the same semantics.
 */
public class DocumentSearchFilterUtils {

  static final String PUBLISHED_LIFECYCLE_STAGE_URN = "urn:li:lifecycleStageType:PUBLISHED";
  static final String DRAFT_LIFECYCLE_STAGE_URN = "urn:li:lifecycleStageType:DRAFT";

  private DocumentSearchFilterUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Builds a combined filter with ownership constraints, optional showInGlobalContext filter, and
   * optional privilege-based bypass of ownership restrictions.
   *
   * @param baseCriteria The base user criteria (without state filtering)
   * @param userAndGroupUrns List of URNs for the current user and their groups
   * @param applyShowInGlobalContext Whether to exclude showInGlobalContext=false documents
   * @param canManageDocuments When true, unpublished documents are visible without ownership
   *     constraints (for users with MANAGE_DOCUMENTS privilege). DRAFT documents remain excluded
   *     regardless.
   * @return The combined filter
   */
  @Nonnull
  public static Filter buildCombinedFilter(
      @Nonnull List<Criterion> baseCriteria,
      @Nonnull List<String> userAndGroupUrns,
      boolean applyShowInGlobalContext,
      boolean canManageDocuments) {

    List<ConjunctiveCriterion> orClauses = new ArrayList<>();

    // --- New lifecycleStage-based clauses (take priority when field is present) ---

    // Clause 1: lifecycleStage = PUBLISHED → visible to all
    List<Criterion> lifecyclePublishedCriteria = new ArrayList<>(baseCriteria);
    lifecyclePublishedCriteria.add(
        CriterionUtils.buildCriterion(
            "lifecycleStage", Condition.EQUAL, PUBLISHED_LIFECYCLE_STAGE_URN));
    if (applyShowInGlobalContext) {
      lifecyclePublishedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(
        new ConjunctiveCriterion().setAnd(new CriterionArray(lifecyclePublishedCriteria)));

    // Clause 2: lifecycleStage != PUBLISHED AND lifecycleStage != DRAFT AND lifecycleStage EXISTS
    // AND (owned OR canManageDocuments) → non-published, non-draft stages (UNPUBLISHED, IN_REVIEW,
    // REJECTED, etc.) visible to owners or users with MANAGE_DOCUMENTS privilege.
    // DRAFT is excluded entirely — it is never surfaced in the UI.
    List<Criterion> lifecycleNonPublishedCriteria = new ArrayList<>(baseCriteria);
    lifecycleNonPublishedCriteria.add(
        buildNegatedCriterion("lifecycleStage", PUBLISHED_LIFECYCLE_STAGE_URN));
    lifecycleNonPublishedCriteria.add(
        buildNegatedCriterion("lifecycleStage", DRAFT_LIFECYCLE_STAGE_URN));
    lifecycleNonPublishedCriteria.add(buildExistsCriterion("lifecycleStage"));
    if (!canManageDocuments) {
      lifecycleNonPublishedCriteria.add(
          CriterionUtils.buildCriterion("owners", Condition.EQUAL, userAndGroupUrns));
    }
    if (applyShowInGlobalContext) {
      lifecycleNonPublishedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(
        new ConjunctiveCriterion().setAnd(new CriterionArray(lifecycleNonPublishedCriteria)));

    // --- Legacy state-based clauses (fallback when lifecycleStage is not set) ---

    // Clause 3: lifecycleStage IS_NULL AND state = PUBLISHED → visible to all
    List<Criterion> legacyPublishedCriteria = new ArrayList<>(baseCriteria);
    legacyPublishedCriteria.add(buildIsNullCriterion("lifecycleStage"));
    legacyPublishedCriteria.add(
        CriterionUtils.buildCriterion("state", Condition.EQUAL, "PUBLISHED"));
    if (applyShowInGlobalContext) {
      legacyPublishedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(legacyPublishedCriteria)));

    // Clause 4: lifecycleStage IS_NULL AND state = UNPUBLISHED AND (owned OR canManageDocuments)
    List<Criterion> legacyUnpublishedCriteria = new ArrayList<>(baseCriteria);
    legacyUnpublishedCriteria.add(buildIsNullCriterion("lifecycleStage"));
    legacyUnpublishedCriteria.add(
        CriterionUtils.buildCriterion("state", Condition.EQUAL, "UNPUBLISHED"));
    if (!canManageDocuments) {
      legacyUnpublishedCriteria.add(
          CriterionUtils.buildCriterion("owners", Condition.EQUAL, userAndGroupUrns));
    }
    if (applyShowInGlobalContext) {
      legacyUnpublishedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(legacyUnpublishedCriteria)));

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

  /** Builds an EXISTS criterion — matches documents where the field is present / not null. */
  private static Criterion buildExistsCriterion(String field) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.EXISTS);
    return criterion;
  }

  /** Builds an IS_NULL criterion — matches documents where the field is absent / not set. */
  private static Criterion buildIsNullCriterion(String field) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.IS_NULL);
    return criterion;
  }
}
