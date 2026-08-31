package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.query.SearchFlags;
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
 * <p>Currently supports default filters for: - DOCUMENT: Shows only published documents using a
 * two-clause OR filter consistent with the MCP tool's anchor filter pattern:
 *
 * <ol>
 *   <li>lifecycleStage = PUBLISHED (new field present and published)
 *   <li>lifecycleStage IS_NULL AND state != UNPUBLISHED (legacy fallback; non-document entities
 *       also pass through this clause)
 * </ol>
 */
public class DefaultEntityFiltersUtil {

  /**
   * Set of field names used in default entity filters. These fields should be excluded from
   * aggregation results since they are system/hidden filters that shouldn't be shown to users.
   */
  public static final Set<String> DEFAULT_FILTER_FIELDS =
      ImmutableSet.of("state", "lifecycleStage", "showInGlobalContext");

  static final String PUBLISHED_LIFECYCLE_STAGE_URN = "urn:li:lifecycleStageType:PUBLISHED";

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
   * Adds default entity-specific filters with optional privilege-based bypass.
   *
   * @param baseFilter The existing filter to combine with default filters (can be null)
   * @param entityTypes List of entity type names being searched
   * @param applyShowInGlobalContext Whether to filter out showInGlobalContext=false documents
   * @param canManageDocuments When true, unpublished documents are not filtered out (for users with
   *     MANAGE_DOCUMENTS privilege). DRAFT documents remain excluded regardless.
   * @return The combined filter with default entity filters applied
   */
  @Nullable
  public static Filter addDefaultEntityFilters(
      @Nullable Filter baseFilter,
      @Nonnull List<String> entityTypes,
      boolean applyShowInGlobalContext,
      boolean canManageDocuments) {

    // Check if any entity types require default filters
    if (!entityTypes.contains(DOCUMENT_ENTITY_NAME)) {
      return baseFilter;
    }

    // Build default filter for documents (uses negated conditions so non-documents pass through)
    Filter documentDefaults =
        buildDocumentDefaultFilter(applyShowInGlobalContext, canManageDocuments);

    if (baseFilter == null || !baseFilter.hasOr() || baseFilter.getOr().isEmpty()) {
      return documentDefaults;
    }

    return SearchUtils.combineFilters(baseFilter, documentDefaults);
  }

  /**
   * Applies default entity filters (e.g. PUBLISHED-only, showInGlobalContext for documents) to the
   * given filter, respecting search flags and user privileges. Skips filtering only when the caller
   * explicitly requests hidden lifecycle stages.
   *
   * <p>This is the single entry point that all cross-entity search resolvers (search, scroll,
   * aggregate, browse) should use to ensure consistent document visibility filtering.
   *
   * @param baseFilter The existing filter to combine with default filters (can be null)
   * @param entityTypes List of entity type names being searched
   * @param searchFlags The search flags from the request (can be null)
   * @param context The query context for authorization checks
   * @return The combined filter with default entity filters applied
   */
  @Nullable
  public static Filter applyDefaultEntityFilters(
      @Nullable Filter baseFilter,
      @Nonnull List<String> entityTypes,
      @Nullable SearchFlags searchFlags,
      @Nonnull QueryContext context) {
    if (searchFlags != null && Boolean.TRUE.equals(searchFlags.isIncludeHiddenLifecycleStages())) {
      return baseFilter;
    }
    boolean canManageDocs =
        entityTypes.contains(DOCUMENT_ENTITY_NAME)
            && AuthorizationUtils.canManageDocuments(context);
    return addDefaultEntityFilters(baseFilter, entityTypes, true, canManageDocs);
  }

  /**
   * Builds default filter for documents in cross-entity search. Mirrors the MCP tool's anchor
   * filter pattern: {@code lifecycleStage = PUBLISHED OR (lifecycleStage IS NULL AND state !=
   * UNPUBLISHED)}.
   *
   * <p>Clause 1 matches documents with the new lifecycleStage field set to PUBLISHED. Clause 2
   * handles legacy documents (no lifecycleStage) and non-document entities (both fields absent —
   * negated EQUAL and IS_NULL both succeed when the field does not exist).
   *
   * @param applyShowInGlobalContext Whether to filter out showInGlobalContext=false documents
   * @return Filter with document defaults
   */
  private static Filter buildDocumentDefaultFilter(
      boolean applyShowInGlobalContext, boolean canManageDocuments) {
    List<ConjunctiveCriterion> orClauses = new ArrayList<>();

    // Clause 1: lifecycleStage = PUBLISHED
    List<Criterion> publishedCriteria = new ArrayList<>();
    publishedCriteria.add(buildEqualCriterion("lifecycleStage", PUBLISHED_LIFECYCLE_STAGE_URN));
    if (applyShowInGlobalContext) {
      publishedCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(publishedCriteria)));

    // Clause 2: lifecycleStage IS_NULL AND state != UNPUBLISHED (legacy fallback).
    // Non-document entities also match this clause (both fields absent).
    // When canManageDocuments is true, skip the state != UNPUBLISHED filter so
    // privileged users can see unpublished documents.
    List<Criterion> legacyCriteria = new ArrayList<>();
    legacyCriteria.add(buildIsNullCriterion("lifecycleStage"));
    if (!canManageDocuments) {
      legacyCriteria.add(buildNegatedCriterion("state", "UNPUBLISHED"));
    }
    if (applyShowInGlobalContext) {
      legacyCriteria.add(buildNegatedCriterion("showInGlobalContext", "false"));
    }
    orClauses.add(new ConjunctiveCriterion().setAnd(new CriterionArray(legacyCriteria)));

    return new Filter().setOr(new ConjunctiveCriterionArray(orClauses));
  }

  private static Criterion buildEqualCriterion(String field, String value) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(
        new com.linkedin.data.template.StringArray(Collections.singletonList(value)));
    return criterion;
  }

  /** Builds an IS_NULL criterion — matches documents where the field is absent / not set. */
  private static Criterion buildIsNullCriterion(String field) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.IS_NULL);
    return criterion;
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
