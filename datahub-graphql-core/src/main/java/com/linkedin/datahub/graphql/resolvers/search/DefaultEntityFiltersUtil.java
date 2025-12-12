package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility class for adding entity-specific default filters to cross-entity searches.
 *
 * <p>When searching across multiple entity types (e.g., datasets, documents, charts), certain
 * entity types may have default filters that should be applied. This utility ensures those filters
 * only affect the target entity types without excluding other entities from search results.
 *
 * <p>Currently supports default filters for: - DOCUMENT: Filters out unpublished documents and
 * optionally filters out documents not meant for global visibility
 */
public class DefaultEntityFiltersUtil {

  /**
   * Set of field names used in default entity filters. These fields should be excluded from
   * aggregation results since they are system/hidden filters that shouldn't be shown to users.
   */
  public static final Set<String> DEFAULT_FILTER_FIELDS =
      ImmutableSet.of("state", "showInGlobalContext");

  private DefaultEntityFiltersUtil() {
    // Utility class - prevent instantiation
  }

  /**
   * Removes aggregation metadata for fields that are used in default entity filters. These fields
   * are hidden/system filters that shouldn't be shown to users in the facet results.
   *
   * @param searchResult The search result to cleanse
   * @return The same search result with default filter fields removed from aggregations
   */
  @Nonnull
  public static SearchResult removeDefaultFilterFieldsFromAggregations(
      @Nonnull SearchResult searchResult) {
    if (!searchResult.hasMetadata() || !searchResult.getMetadata().hasAggregations()) {
      return searchResult;
    }

    SearchResultMetadata metadata = searchResult.getMetadata();
    AggregationMetadataArray originalAggs = metadata.getAggregations();

    // Filter out aggregations for default filter fields
    List<AggregationMetadata> filteredAggs =
        originalAggs.stream()
            .filter(agg -> !DEFAULT_FILTER_FIELDS.contains(agg.getName()))
            .collect(Collectors.toList());

    // Only update if we actually filtered something out
    if (filteredAggs.size() < originalAggs.size()) {
      metadata.setAggregations(new AggregationMetadataArray(filteredAggs));
    }

    return searchResult;
  }

  /**
   * Removes aggregation metadata for fields that are used in default entity filters. These fields
   * are hidden/system filters that shouldn't be shown to users in the facet results.
   *
   * @param scrollResult The scroll result to cleanse
   * @return The same scroll result with default filter fields removed from aggregations
   */
  @Nonnull
  public static ScrollResult removeDefaultFilterFieldsFromAggregations(
      @Nonnull ScrollResult scrollResult) {
    if (!scrollResult.hasMetadata() || !scrollResult.getMetadata().hasAggregations()) {
      return scrollResult;
    }

    SearchResultMetadata metadata = scrollResult.getMetadata();
    AggregationMetadataArray originalAggs = metadata.getAggregations();

    // Filter out aggregations for default filter fields
    List<AggregationMetadata> filteredAggs =
        originalAggs.stream()
            .filter(agg -> !DEFAULT_FILTER_FIELDS.contains(agg.getName()))
            .collect(Collectors.toList());

    // Only update if we actually filtered something out
    if (filteredAggs.size() < originalAggs.size()) {
      metadata.setAggregations(new AggregationMetadataArray(filteredAggs));
    }

    return scrollResult;
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
}
