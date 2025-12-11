package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility class for adding entity-specific default filters to cross-entity searches.
 *
 * <p>When searching across multiple entity types (e.g., datasets, documents, charts), certain
 * entity types may have default filters that should be applied. This utility ensures those filters
 * only affect the target entity types without excluding other entities from search results.
 *
 * <p>Currently supports default filters for: - DOCUMENT: Filters out unpublished documents,
 * excludes drafts, and optionally filters out documents not meant for global visibility
 */
public class DefaultEntityFiltersUtil {

  private DefaultEntityFiltersUtil() {
    // Utility class - prevent instantiation
  }

  /**
   * Adds default entity-specific filters to the base filter for cross-entity searches.
   *
   * <p>The filters use negated EQUAL conditions which naturally pass through for entities that
   * don't have the filtered fields. For example, `state != UNPUBLISHED` (negated EQUAL) passes for
   * datasets (which don't have a state field) because the field doesn't exist with the excluded
   * value.
   *
   * @param baseFilter The existing filter to combine with default filters (can be null)
   * @param entityTypes List of entity type names being searched
   * @param applyShowInGlobalContext Whether to filter out showInGlobalContext=false documents
   * @return The combined filter with default entity filters applied
   */
  @Nullable
  public static Filter addDefaultEntityFilters(
      @Nullable Filter baseFilter,
      @Nonnull List<String> entityTypes,
      boolean applyShowInGlobalContext) {

    // Check if any entity types require default filters
    if (!entityTypes.contains(DOCUMENT_ENTITY_NAME)) {
      return baseFilter;
    }

    // Build default filter for documents (uses negated conditions so non-documents pass through)
    Filter documentDefaults = buildDocumentDefaultFilter(applyShowInGlobalContext);

    if (baseFilter == null || !baseFilter.hasOr() || baseFilter.getOr().isEmpty()) {
      return documentDefaults;
    }

    return SearchUtils.combineFilters(baseFilter, documentDefaults);
  }

  /**
   * Builds default filter for documents using negated EQUAL conditions.
   *
   * <p>Using negated conditions allows non-document entities to naturally pass through:
   *
   * <ul>
   *   <li>`state != UNPUBLISHED` (negated) - passes for non-documents (field doesn't exist) and
   *       published documents
   *   <li>`showInGlobalContext != false` (negated) - passes for non-documents and documents meant
   *       for global visibility
   *   <li>`draftOf IS_NULL` - passes for non-documents and non-draft documents
   * </ul>
   *
   * @param applyShowInGlobalContext Whether to filter out showInGlobalContext=false documents
   * @return Filter with document defaults using negated conditions
   */
  private static Filter buildDocumentDefaultFilter(boolean applyShowInGlobalContext) {
    List<Criterion> criteria = new ArrayList<>();

    // Exclude unpublished documents (non-documents pass through since they don't have this field)
    // Using negated EQUAL instead of NOT_EQUAL
    criteria.add(buildNegatedCriterion("state", "UNPUBLISHED"));

    // Optionally exclude documents not meant for global context
    if (applyShowInGlobalContext) {
      criteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }

    // Exclude draft documents (draftOf field must be null/not set)
    criteria.add(buildIsNullCriterion("draftOf"));

    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
  }

  /**
   * Helper to build a negated Criterion (field != value).
   *
   * @param field The field name
   * @param value The value to exclude
   * @return A Criterion with EQUAL condition and negated=true
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

  /**
   * Helper to build a Criterion that checks if a field is null or doesn't exist.
   *
   * @param field The field name to check
   * @return A Criterion with IS_NULL condition
   */
  private static Criterion buildIsNullCriterion(String field) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.IS_NULL);
    return criterion;
  }
}
