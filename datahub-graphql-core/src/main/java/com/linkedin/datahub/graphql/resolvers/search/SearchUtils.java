package com.linkedin.datahub.graphql.resolvers.search;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.view.DataHubViewInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.util.CollectionUtils;

@Slf4j
public class SearchUtils {
  private SearchUtils() {}

  private static final int DEFAULT_SEARCH_COUNT = 10;

  /** Search-document field on structured properties listing the entity type URNs they apply to. */
  public static final String STRUCTURED_PROPERTY_ENTITY_TYPES_FIELD = "entityTypes";

  private static final int DEFAULT_SCROLL_COUNT = 10;
  private static final String DEFAULT_SCROLL_KEEP_ALIVE = "5m";

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

  /**
   * Maps GraphQL entity types to registry names. When {@code inputTypes} is null/empty, returns the
   * configured default search entity types from the operation context (resolved from {@code
   * elasticsearch.search.defaultEntityTypes}). Explicit non-empty {@code inputTypes} always win.
   *
   * <p>An empty returned list means <em>search no entity types</em>. Callers must short-circuit to
   * empty results and must not pass an empty list into SearchService APIs that treat empty as "all
   * non-empty indices" (Rest.li omit-entities semantics).
   */
  public static List<String> getSearchEntityNames(@Nonnull OperationContext opContext) {
    return getSearchEntityNames(opContext, null);
  }

  public static List<String> getSearchEntityNames(@Nullable List<EntityType> inputTypes) {
    return getSearchEntityNames(null, inputTypes);
  }

  public static List<String> getSearchEntityNames(
      @Nullable OperationContext opContext, @Nullable List<EntityType> inputTypes) {
    if (inputTypes != null && !inputTypes.isEmpty()) {
      return inputTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());
    }
    return cachedEntityTypeList(opContext, resolvedCachedList(opContext, CachedList.SEARCH));
  }

  /**
   * Default autocomplete entity registry names from {@code
   * elasticsearch.search.autocompleteEntityTypes}.
   */
  @Nonnull
  public static List<String> getAutocompleteEntityNames(@Nullable OperationContext opContext) {
    return cachedEntityTypeList(opContext, resolvedCachedList(opContext, CachedList.AUTOCOMPLETE));
  }

  /** Default browse entity registry names from {@code elasticsearch.search.browseEntityTypes}. */
  @Nonnull
  public static List<String> getBrowseEntityNames(@Nullable OperationContext opContext) {
    return cachedEntityTypeList(opContext, resolvedCachedList(opContext, CachedList.BROWSE));
  }

  /**
   * Prioritized source-entity types for quick filters from {@code
   * elasticsearch.search.prioritizedSourceEntityTypes}.
   */
  @Nonnull
  public static List<String> getPrioritizedSourceEntityTypes(@Nullable OperationContext opContext) {
    return cachedEntityTypeList(
        opContext, resolvedCachedList(opContext, CachedList.PRIORITIZED_SOURCE));
  }

  /**
   * Prioritized DataHub-entity types for quick filters from {@code
   * elasticsearch.search.prioritizedDatahubEntityTypes}.
   */
  @Nonnull
  public static List<String> getPrioritizedDatahubEntityTypes(
      @Nullable OperationContext opContext) {
    return cachedEntityTypeList(
        opContext, resolvedCachedList(opContext, CachedList.PRIORITIZED_DATAHUB));
  }

  private enum CachedList {
    SEARCH,
    AUTOCOMPLETE,
    BROWSE,
    PRIORITIZED_SOURCE,
    PRIORITIZED_DATAHUB
  }

  @Nullable
  private static List<String> resolvedCachedList(
      @Nullable OperationContext opContext, @Nonnull CachedList which) {
    if (opContext == null || opContext.getSearchContext() == null) {
      return null;
    }
    return switch (which) {
      case SEARCH -> opContext.getSearchContext().getDefaultSearchEntityNames();
      case AUTOCOMPLETE -> opContext.getSearchContext().getDefaultAutocompleteEntityNames();
      case BROWSE -> opContext.getSearchContext().getDefaultBrowseEntityNames();
      case PRIORITIZED_SOURCE -> opContext.getSearchContext().getPrioritizedSourceEntityTypes();
      case PRIORITIZED_DATAHUB -> opContext.getSearchContext().getPrioritizedDatahubEntityTypes();
    };
  }

  @Nonnull
  private static List<String> cachedEntityTypeList(
      @Nullable OperationContext opContext, @Nullable List<String> cached) {
    if (cached != null) {
      return cached;
    }
    if (opContext != null) {
      log.warn(
          "Search entity-type list was not resolved onto SearchContext; returning empty "
              + "(search no entity types, not all indices). Ensure elasticsearch.search.*EntityTypes "
              + "is configured.");
    }
    return List.of();
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

  public static ScrollResults createEmptyScrollResults(final int count) {
    final ScrollResults result = new ScrollResults();
    result.setCount(count);
    result.setTotal(0);
    result.setSearchResults(new ArrayList<>());
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

    final List<String> entityNames =
        getSearchEntityNames(inputContext.getOperationContext(), inputEntityTypes);

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

          if (finalEntityNames.isEmpty()) {
            log.debug(
                "scrollAcrossEntities: empty entity-type list; returning no results "
                    + "(not searching all indices)");
            return createEmptyScrollResults(count);
          }

          final Filter finalFilters =
              maybeResolvedView
                  .map(
                      (view) ->
                          FilterUtils.combineFilters(baseFilter, view.getDefinition().getFilter()))
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
                    context,
                    finalEntityNames,
                    query,
                    finalFilters,
                    scrollId,
                    keepAlive,
                    null,
                    count);
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

    final List<String> entityNames =
        getSearchEntityNames(inputContext.getOperationContext(), inputEntityTypes);

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

          if (finalEntityNames.isEmpty()) {
            log.debug(
                "searchAcrossEntities: empty entity-type list; returning no results "
                    + "(not searching all indices)");
            return createEmptySearchResults(start, count);
          }

          final Filter finalFilters =
              maybeResolvedView
                  .map(
                      (view) ->
                          FilterUtils.combineFilters(baseFilter, view.getDefinition().getFilter()))
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
                    context, finalEntityNames, query, finalFilters, start, count, sortCriteria);
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

  /**
   * Doc-level, fail-open check of whether a structured property applies to any of the searched
   * entity types. {@code entityTypesJson} is the JSON-stringified {@code entityTypes} field from
   * the structured property's search document: a list of entity type URNs such as {@code
   * urn:li:entityType:datahub.dataset}. A property is kept when the searched entity list is empty,
   * when entityTypes is missing/empty/unparseable (fail open: an extra facet for an unrelated type
   * is harmless, a missing facet is not), or when at least one of its entity types matches a
   * searched entity name.
   */
  public static boolean structuredPropertyAppliesToEntityTypes(
      @Nonnull final ObjectMapper objectMapper,
      @Nullable final String entityTypesJson,
      @Nonnull final List<String> searchedEntityNames) {
    if (searchedEntityNames.isEmpty() || entityTypesJson == null) {
      return true;
    }
    final List<String> entityTypeUrns;
    try {
      entityTypeUrns = objectMapper.readValue(entityTypesJson, new TypeReference<>() {});
    } catch (Exception e) {
      log.warn(
          "Failed to parse structured property entityTypes {}; keeping facet", entityTypesJson, e);
      return true;
    }
    if (entityTypeUrns == null || entityTypeUrns.isEmpty()) {
      return true;
    }
    final Set<String> searchedNames =
        searchedEntityNames.stream()
            .map(name -> name.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    // Entity type URNs come in both the modern urn:li:entityType:datahub.dataset and the legacy
    // urn:li:entityType:dataset forms. Reuse the canonical parser (same helper the mapping side
    // uses) so both resolve; null/unparseable entries are kept (fail open).
    return entityTypeUrns.stream()
        .anyMatch(
            typeUrn -> {
              if (typeUrn == null) {
                return true;
              }
              final String entityName;
              try {
                entityName = StructuredPropertyUtils.getEntityTypeId(UrnUtils.getUrn(typeUrn));
              } catch (Exception e) {
                return true;
              }
              return entityName == null
                  || searchedNames.contains(entityName.toLowerCase(Locale.ROOT));
            });
  }
}
