package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_FLOW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_FEATURE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_FEATURE_TABLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_PRIMARY_KEY_ENTITY_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnScrollResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.util.CollectionUtils;

@Slf4j
public class SearchUtils {
  private SearchUtils() {}

  private static final int DEFAULT_SEARCH_COUNT = 10;
  private static final int DEFAULT_SCROLL_COUNT = 10;
  private static final String DEFAULT_SCROLL_KEEP_ALIVE = "5m";

  /** Entities that are searched by default in Search Across Entities */
  public static final List<EntityType> SEARCHABLE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.MLFEATURE,
          EntityType.MLPRIMARY_KEY,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM,
          EntityType.GLOSSARY_NODE,
          EntityType.TAG,
          EntityType.ROLE,
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.CONTAINER,
          EntityType.DOMAIN,
          EntityType.DATA_PRODUCT,
          EntityType.NOTEBOOK,
          EntityType.BUSINESS_ATTRIBUTE,
          EntityType.SCHEMA_FIELD,
          EntityType.DATA_PLATFORM_INSTANCE);

  /** Entities that are part of autocomplete by default in Auto Complete Across Entities */
  public static final List<EntityType> AUTO_COMPLETE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.CONTAINER,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM,
          EntityType.TAG,
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.NOTEBOOK,
          EntityType.DATA_PRODUCT,
          EntityType.DOMAIN,
          EntityType.BUSINESS_ATTRIBUTE);

  /** Entities that are part of browse by default */
  public static final List<EntityType> BROWSE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.CONTAINER,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.NOTEBOOK);

  /** A prioritized list of source filter types used to generate quick filters */
  public static final List<String> PRIORITIZED_SOURCE_ENTITY_TYPES =
      Stream.of(
              DATASET_ENTITY_NAME,
              DASHBOARD_ENTITY_NAME,
              DATA_FLOW_ENTITY_NAME,
              DATA_JOB_ENTITY_NAME,
              CHART_ENTITY_NAME,
              CONTAINER_ENTITY_NAME,
              ML_MODEL_ENTITY_NAME,
              ML_MODEL_GROUP_ENTITY_NAME,
              ML_FEATURE_ENTITY_NAME,
              ML_FEATURE_TABLE_ENTITY_NAME,
              ML_PRIMARY_KEY_ENTITY_NAME)
          .map(String::toLowerCase)
          .collect(Collectors.toList());

  /** A prioritized list of DataHub filter types used to generate quick filters */
  public static final List<String> PRIORITIZED_DATAHUB_ENTITY_TYPES =
      Stream.of(
              DOMAIN_ENTITY_NAME,
              GLOSSARY_TERM_ENTITY_NAME,
              CORP_GROUP_ENTITY_NAME,
              CORP_USER_ENTITY_NAME)
          .map(String::toLowerCase)
          .collect(Collectors.toList());

  /**
   * Combines two {@link Filter} instances in a conjunction and returns a new instance of {@link
   * Filter} in disjunctive normal form.
   *
   * @param baseFilter the filter to apply the view to
   * @param viewFilter the view filter, null if it doesn't exist
   * @return a new instance of {@link Filter} representing the applied view.
   */
  @Nonnull
  public static Filter combineFilters(
      @Nullable final Filter baseFilter, @Nonnull final Filter viewFilter) {
    final Filter finalBaseFilter =
        baseFilter == null
            ? new Filter().setOr(new ConjunctiveCriterionArray(Collections.emptyList()))
            : baseFilter;

    // Join the filter conditions in Disjunctive Normal Form.
    return combineFiltersInConjunction(finalBaseFilter, viewFilter);
  }

  /**
   * Returns the intersection of two sets of entity types. (Really just string lists). If either is
   * empty, consider the entity types list to mean "all" (take the other set).
   *
   * @param baseEntityTypes the entity types to apply the view to
   * @param viewEntityTypes the view info, null if it doesn't exist
   * @return the intersection of the two input sets
   */
  @Nonnull
  public static List<String> intersectEntityTypes(
      @Nonnull final List<String> baseEntityTypes, @Nonnull final List<String> viewEntityTypes) {
    if (baseEntityTypes.isEmpty()) {
      return viewEntityTypes;
    }
    if (viewEntityTypes.isEmpty()) {
      return baseEntityTypes;
    }
    // Join the entity types in intersection.
    return new ArrayList<>(CollectionUtils.intersection(baseEntityTypes, viewEntityTypes));
  }

  /**
   * Joins two filters in conjunction by reducing to Disjunctive Normal Form.
   *
   * @param filter1 the first filter in the pair
   * @param filter2 the second filter in the pair
   *     <p>This method supports either Filter format, where the "or" field is used, instead of
   *     criteria. If the criteria filter is used, then it will be converted into an "OR" before
   *     returning the new filter.
   * @return the result of joining the 2 filters in a conjunction (AND)
   *     <p>How does it work? It basically cross-products the conjunctions inside of each Filter
   *     clause.
   *     <p>Example Inputs: filter1 -> { or: [ { and: [ { field: tags, condition: EQUAL, values:
   *     ["urn:li:tag:tag"] } ] }, { and: [ { field: glossaryTerms, condition: EQUAL, values:
   *     ["urn:li:glossaryTerm:term"] } ] } ] } filter2 -> { or: [ { and: [ { field: domain,
   *     condition: EQUAL, values: ["urn:li:domain:domain"] }, ] }, { and: [ { field: glossaryTerms,
   *     condition: EQUAL, values: ["urn:li:glossaryTerm:term2"] } ] } ] } Example Output: { or: [ {
   *     and: [ { field: tags, condition: EQUAL, values: ["urn:li:tag:tag"] }, { field: domain,
   *     condition: EQUAL, values: ["urn:li:domain:domain"] } ] }, { and: [ { field: tags,
   *     condition: EQUAL, values: ["urn:li:tag:tag"] }, { field: glossaryTerms, condition: EQUAL,
   *     values: ["urn:li:glosaryTerm:term2"] } ] }, { and: [ { field: glossaryTerm, condition:
   *     EQUAL, values: ["urn:li:glossaryTerm:term"] }, { field: domain, condition: EQUAL, values:
   *     ["urn:li:domain:domain"] } ] }, { and: [ { field: glossaryTerm, condition: EQUAL, values:
   *     ["urn:li:glossaryTerm:term"] }, { field: glossaryTerms, condition: EQUAL, values:
   *     ["urn:li:glosaryTerm:term2"] } ] }, ] }
   */
  @Nonnull
  private static Filter combineFiltersInConjunction(
      @Nonnull final Filter filter1, @Nonnull final Filter filter2) {

    final Filter finalFilter1 = convertToV2Filter(filter1);
    final Filter finalFilter2 = convertToV2Filter(filter2);

    // If either filter is empty, simply return the other filter.
    if (!finalFilter1.hasOr() || finalFilter1.getOr().size() == 0) {
      return finalFilter2;
    }
    if (!finalFilter2.hasOr() || finalFilter2.getOr().size() == 0) {
      return finalFilter1;
    }

    // Iterate through the base filter, then cross-product with filter 2 conditions.
    final Filter result = new Filter();
    final List<ConjunctiveCriterion> newDisjunction = new ArrayList<>();
    for (ConjunctiveCriterion conjunction1 : finalFilter1.getOr()) {
      for (ConjunctiveCriterion conjunction2 : finalFilter2.getOr()) {
        final List<Criterion> joinedCriterion = new ArrayList<>(conjunction1.getAnd());
        joinedCriterion.addAll(conjunction2.getAnd());
        ConjunctiveCriterion newConjunction =
            new ConjunctiveCriterion().setAnd(new CriterionArray(joinedCriterion));
        newDisjunction.add(newConjunction);
      }
    }
    result.setOr(new ConjunctiveCriterionArray(newDisjunction));
    return result;
  }

  @Nonnull
  private static Filter convertToV2Filter(@Nonnull Filter filter) {
    if (filter.hasOr()) {
      return filter;
    } else if (filter.hasCriteria()) {
      // Convert criteria to an OR
      return new Filter()
          .setOr(
              new ConjunctiveCriterionArray(
                  ImmutableList.of(new ConjunctiveCriterion().setAnd(filter.getCriteria()))));
    }
    throw new IllegalArgumentException(
        String.format(
            "Illegal filter provided! Neither 'or' nor 'criteria' fields were populated for filter %s",
            filter));
  }

  /**
   * Attempts to resolve a View by urn. Throws {@link IllegalArgumentException} if a View with the
   * specified urn cannot be found.
   */
  public static DataHubViewInfo resolveView(
      @Nonnull OperationContext opContext,
      @Nonnull ViewService viewService,
      @Nonnull final Urn viewUrn) {
    try {
      DataHubViewInfo maybeViewInfo = viewService.getViewInfo(opContext, viewUrn);
      if (maybeViewInfo == null) {
        log.warn(
            String.format("Failed to resolve View with urn %s. View does not exist!", viewUrn));
      }
      return maybeViewInfo;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while attempting to resolve View with URN %s", viewUrn),
          e);
    }
  }

  //  Assumption is that filter values for degree are either null, 3+, 2, or 1.
  public static Integer getMaxHops(List<FacetFilterInput> filters) {
    Set<String> degreeFilterValues =
        filters.stream()
            .filter(filter -> filter.getField().equals("degree"))
            .flatMap(filter -> filter.getValues().stream())
            .collect(Collectors.toSet());
    Integer maxHops = null;
    if (!degreeFilterValues.contains("3+")) {
      if (degreeFilterValues.contains("2")) {
        maxHops = 2;
      } else if (degreeFilterValues.contains("1")) {
        maxHops = 1;
      }
    }
    return maxHops;
  }

  public static SearchFlags mapInputFlags(
      @Nullable QueryContext context,
      com.linkedin.datahub.graphql.generated.SearchFlags inputFlags) {
    SearchFlags searchFlags = null;
    if (inputFlags != null) {
      searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
    }
    return searchFlags;
  }

  public static SortCriterion mapSortCriterion(
      com.linkedin.datahub.graphql.generated.SortCriterion sortCriterion) {
    SortCriterion result = new SortCriterion();
    result.setField(sortCriterion.getField());
    result.setOrder(SortOrder.valueOf(sortCriterion.getSortOrder().name()));
    return result;
  }

  public static List<String> getEntityNames(List<EntityType> inputTypes) {
    final List<EntityType> entityTypes =
        (inputTypes == null || inputTypes.isEmpty()) ? SEARCHABLE_ENTITY_TYPES : inputTypes;
    return entityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());
  }

  public static SearchResults createEmptySearchResults(final int start, final int count) {
    final SearchResults result = new SearchResults();
    result.setStart(start);
    result.setCount(count);
    result.setTotal(0);
    result.setSearchResults(new ArrayList<>());
    result.setSuggestions(new ArrayList<>());
    result.setFacets(new ArrayList<>());
    return result;
  }

  public static List<SortCriterion> getSortCriteria(@Nullable final SearchSortInput sortInput) {
    List<SortCriterion> sortCriteria;
    if (sortInput != null) {
      if (sortInput.getSortCriteria() != null) {
        sortCriteria =
            sortInput.getSortCriteria().stream()
                .map(SearchUtils::mapSortCriterion)
                .collect(Collectors.toList());
      } else {
        sortCriteria =
            sortInput.getSortCriterion() != null
                ? Collections.singletonList(mapSortCriterion(sortInput.getSortCriterion()))
                : new ArrayList<>();
      }
    } else {
      sortCriteria = new ArrayList<>();
    }

    return sortCriteria;
  }

  public static CompletableFuture<ScrollResults> scrollAcrossEntities(
      QueryContext inputContext,
      final EntityClient _entityClient,
      final ViewService _viewService,
      List<EntityType> inputEntityTypes,
      String inputQuery,
      Filter baseFilter,
      String viewUrn,
      com.linkedin.datahub.graphql.generated.SearchFlags inputSearchFlags,
      Integer inputCount,
      String scrollId,
      String inputKeepAlive,
      String className) {

    final List<EntityType> entityTypes =
        (inputEntityTypes == null || inputEntityTypes.isEmpty())
            ? SEARCHABLE_ENTITY_TYPES
            : inputEntityTypes;
    final List<String> entityNames =
        entityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());

    // escape forward slash since it is a reserved character in Elasticsearch, default to * if
    // blank/empty
    final String query =
        StringUtils.isNotBlank(inputQuery) ? ResolverUtils.escapeForwardSlash(inputQuery) : "*";

    final Optional<SearchFlags> searchFlags =
        Optional.ofNullable(inputSearchFlags)
            .map((flags) -> SearchFlagsInputMapper.map(inputContext, flags));
    final OperationContext context =
        inputContext.getOperationContext().withSearchFlags(searchFlags::orElse);

    final int count = Optional.ofNullable(inputCount).orElse(DEFAULT_SCROLL_COUNT);
    final String keepAlive = Optional.ofNullable(inputKeepAlive).orElse(DEFAULT_SCROLL_KEEP_ALIVE);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final OperationContext baseContext = inputContext.getOperationContext();
          final Optional<DataHubViewInfo> maybeResolvedView =
              Optional.ofNullable(viewUrn)
                  .map((urn) -> resolveView(baseContext, _viewService, UrnUtils.getUrn(urn)));

          final List<String> finalEntityNames =
              maybeResolvedView
                  .map(
                      (view) ->
                          intersectEntityTypes(entityNames, view.getDefinition().getEntityTypes()))
                  .orElse(entityNames);

          final Filter finalFilters =
              maybeResolvedView
                  .map((view) -> combineFilters(baseFilter, view.getDefinition().getFilter()))
                  .orElse(baseFilter);

          log.debug(
              "Executing search for multiple entities: entity types {}, query {}, filters: {}, scrollId: {}, count: {}",
              finalEntityNames,
              query,
              finalFilters,
              scrollId,
              count);

          try {
            final ScrollResult scrollResult =
                _entityClient.scrollAcrossEntities(
                    context, finalEntityNames, query, finalFilters, scrollId, keepAlive, count);
            return UrnScrollResultsMapper.map(inputContext, scrollResult);
          } catch (Exception e) {
            log.warn(
                "Failed to execute search for multiple entities: entity types {}, query {}, filters: {}, searchAfter: {}, count: {}",
                finalEntityNames,
                query,
                finalFilters,
                scrollId,
                count);
            throw new RuntimeException(
                "Failed to execute search: "
                    + String.format(
                        "entity types %s, query %s, filters: %s, start: %s, count: %s",
                        finalEntityNames, query, finalFilters, scrollId, count),
                e);
          }
        },
        className,
        "scrollAcrossEntities");
  }

  public static CompletableFuture<SearchResults> searchAcrossEntities(
      QueryContext inputContext,
      final EntityClient _entityClient,
      final ViewService _viewService,
      List<EntityType> inputEntityTypes,
      String inputQuery,
      Filter baseFilter,
      String viewUrn,
      List<SortCriterion> sortCriteria,
      com.linkedin.datahub.graphql.generated.SearchFlags inputSearchFlags,
      Integer inputCount,
      Integer inputStart,
      String className) {

    final List<EntityType> entityTypes =
        (inputEntityTypes == null || inputEntityTypes.isEmpty())
            ? SEARCHABLE_ENTITY_TYPES
            : inputEntityTypes;
    final List<String> entityNames =
        entityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());

    // escape forward slash since it is a reserved character in Elasticsearch, default to * if
    // blank/empty
    final String query =
        StringUtils.isNotBlank(inputQuery) ? ResolverUtils.escapeForwardSlash(inputQuery) : "*";

    final Optional<SearchFlags> searchFlags =
        Optional.ofNullable(inputSearchFlags)
            .map((flags) -> SearchFlagsInputMapper.map(inputContext, flags));
    final OperationContext context =
        inputContext.getOperationContext().withSearchFlags(searchFlags::orElse);

    final int count = Optional.ofNullable(inputCount).orElse(DEFAULT_SEARCH_COUNT);
    final int start = Optional.ofNullable(inputStart).orElse(0);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final OperationContext baseContext = inputContext.getOperationContext();
          final Optional<DataHubViewInfo> maybeResolvedView =
              Optional.ofNullable(viewUrn)
                  .map((urn) -> resolveView(baseContext, _viewService, UrnUtils.getUrn(urn)));

          final List<String> finalEntityNames =
              maybeResolvedView
                  .map(
                      (view) ->
                          intersectEntityTypes(entityNames, view.getDefinition().getEntityTypes()))
                  .orElse(entityNames);

          final Filter finalFilters =
              maybeResolvedView
                  .map((view) -> combineFilters(baseFilter, view.getDefinition().getFilter()))
                  .orElse(baseFilter);

          log.debug(
              "Executing search for multiple entities: entity types {}, query {}, filters: {}, start: {}, count: {}",
              finalEntityNames,
              query,
              finalFilters,
              start,
              count);

          try {
            final SearchResult searchResult =
                _entityClient.searchAcrossEntities(
                    context,
                    finalEntityNames,
                    query,
                    finalFilters,
                    start,
                    count,
                    sortCriteria,
                    null);
            return UrnSearchResultsMapper.map(inputContext, searchResult);
          } catch (Exception e) {
            log.warn(
                "Failed to execute search for multiple entities: entity types {}, query {}, filters: {}, start: {}, count: {}",
                finalEntityNames,
                query,
                finalFilters,
                start,
                count);
            throw new RuntimeException(
                "Failed to execute search: "
                    + String.format(
                        "entity types %s, query %s, filters: %s, start: %s, count: %s",
                        finalEntityNames, query, finalFilters, start, count),
                e);
          }
        },
        className,
        "searchAcrossEntities");
  }
}
