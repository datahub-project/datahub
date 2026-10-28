package com.linkedin.metadata.search.elasticsearch.query;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.request.PreparedSearchRequest;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponent;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentPage;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRequest;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponents;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRollout;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRuntimes;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateWalk;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchOperation;
import com.linkedin.metadata.search.utils.SearchTookMetrics;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.search.template.SearchTemplateComponentMode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Runs a search, scroll, filter, or explain through the search-template ladder. {@link ESSearchDAO}
 * keeps the request entry points and delegates the template path here.
 */
@Slf4j
final class TemplateSearchExecutor {
  private final ESSearchDAO dao;

  TemplateSearchExecutor(ESSearchDAO dao) {
    this.dao = dao;
  }

  /**
   * The template query a search or scroll for the same input would serve. Scroll renders the routed
   * seed and does not walk {@code next}. A search walks the ladder, including a page of size zero,
   * which fails the light page-size select and continues.
   */
  @Nonnull
  QueryBuilder templateExplainQuery(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nonnull String entityName,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    boolean scroll = scrollId != null || keepAlive != null;
    List<String> entityNames = List.of(entityName);
    OperationContext bound = bindTemplates(opContext, query, scroll, entityNames);
    ResolvedSearchTemplate resolved = bound.getSearchContext().getResolvedSearchTemplate();
    String renderUrn = resolved == null ? null : resolved.getUrn();
    if (!scroll && renderUrn != null) {
      SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
      SearchTemplateWalk.Served<SearchResult> served =
          SearchTemplateWalk.walk(
              bound,
              coordinator.loader(),
              coordinator.ladderExecutor(),
              renderUrn,
              new SearchTemplateWalk.Request(
                  query, postFilters, sortCriteria, size, SearchTemplateWalk.identity()),
              urn ->
                  searchAttempt(
                      bound, urn, entityNames, query, postFilters, sortCriteria, 0, size, facets),
              (first, entities, total) -> {
                SearchResult copy = new SearchResult();
                copy.setEntities(new SearchEntityArray(entities));
                copy.setMetadata(first.getMetadata());
                copy.setFrom(first.getFrom());
                copy.setPageSize(first.getPageSize());
                copy.setNumEntities(total);
                return copy;
              });
      if (served.urn() != null) {
        renderUrn = served.urn();
      }
    }
    return prepareTemplateSearch(
            bound, entityNames, query, postFilters, sortCriteria, 0, size, facets, renderUrn)
        .prepared()
        .request()
        .source()
        .query();
  }

  /**
   * The query a search for the same input would serve: the light query when it matches anything
   * (counted without fetching hits), else the full query, as {@link ESSearchDAO#searchLightFirst}
   * decides.
   */
  QueryBuilder servedQuery(
      @Nonnull OperationContext opContext,
      @Nonnull String index,
      @Nonnull QueryBuilder fullQuery,
      @Nullable QueryBuilder lightQuery,
      @Nonnull String input)
      throws IOException {
    if (lightQuery == null) {
      return fullQuery;
    }
    QueryBuilder lightSourceQuery = ESSearchDAO.buildLightSourceQuery(fullQuery, lightQuery);
    if (lightSourceQuery == null) {
      return fullQuery;
    }
    SearchRequest countRequest =
        new SearchRequest(index)
            .source(
                new SearchSourceBuilder().query(lightSourceQuery).size(0).trackTotalHitsUpTo(1));
    SearchResponse count =
        SearchTookMetrics.time(
            opContext,
            SearchOperation.ENTITY_SEARCH,
            () ->
                dao.searchClient(opContext, countRequest)
                    .search(opContext, countRequest, RequestOptions.DEFAULT));
    // The decision searchLightFirst makes
    return ESSearchDAO.hasHits(count) || ESSearchDAO.stopsOnEmpty(count, input)
        ? lightSourceQuery
        : fullQuery;
  }

  record SearchExecution<T>(
      @Nonnull T page, boolean shardFailed, @Nullable RenderedSearchTemplate renderedTemplate) {}

  record PreparedRequestComponents(
      @Nonnull PreparedSearchRequest prepared,
      @Nullable Filter filter,
      @Nonnull List<EntitySpec> entitySpecs) {}

  /** The context {@code armServed} updated, plus the page that search returned. */
  record BoundSearch(@Nonnull OperationContext context, @Nonnull SearchResult result) {}

  @Nonnull
  BoundSearch searchWithTemplates(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    if (coordinator == null || !coordinator.executes(opContext)) {
      return new BoundSearch(
          opContext,
          searchOnce(opContext, entityNames, input, postFilters, sortCriteria, from, size, facets));
    }
    OperationContext bound = bindTemplates(opContext, input, false, entityNames);
    ResolvedSearchTemplate resolved = bound.getSearchContext().getResolvedSearchTemplate();
    if (resolved == null) {
      return new BoundSearch(
          bound,
          searchOnce(bound, entityNames, input, postFilters, sortCriteria, from, size, facets));
    }
    SearchTemplateWalk.Served<SearchResult> served =
        SearchTemplateWalk.walk(
            bound,
            coordinator.loader(),
            coordinator.ladderExecutor(),
            resolved.getUrn(),
            new SearchTemplateWalk.Request(
                input,
                postFilters,
                sortCriteria,
                size,
                (urn, raw) ->
                    componentReorder(
                        bound, urn, raw, input, postFilters, sortCriteria, from, size)),
            urn ->
                searchAttempt(
                    bound, urn, entityNames, input, postFilters, sortCriteria, from, size, facets),
            (first, entities, total) -> {
              SearchResult copy = new SearchResult();
              copy.setEntities(new SearchEntityArray(entities));
              copy.setMetadata(first.getMetadata());
              copy.setFrom(first.getFrom());
              copy.setPageSize(first.getPageSize());
              copy.setNumEntities(total);
              return copy;
            });
    ResolvedSearchTemplate current = bound.getSearchContext().getResolvedSearchTemplate();
    if (current != null) {
      current.setHighlightFields(served.highlights());
    }
    coordinator.armServed(bound, ESSearchDAO.isRelevanceSort(sortCriteria) ? served.urn() : null);
    return new BoundSearch(bound, served.page());
  }

  /**
   * Binds the shipped or stored template when the caller has not already chosen one. Search through
   * the cache binds first; a direct search still has to, or the ladder never runs.
   */
  @Nonnull
  private OperationContext bindTemplates(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean scroll,
      @Nonnull List<String> entityNames) {
    if (opContext.getSearchContext().getResolvedSearchTemplate() != null) {
      return opContext;
    }
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    if (coordinator == null) {
      return opContext;
    }
    return coordinator.bind(opContext, query, scroll, entityNames);
  }

  @Nonnull
  private SearchTemplateWalk.AttemptResult<SearchResult> searchAttempt(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    try {
      SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
      int fetchFrom = from;
      Integer fetchSize = size;
      if (coordinator != null) {
        MergedSearchTemplate template = coordinator.loader().load(opContext, urn);
        com.linkedin.search.template.SearchTemplateComponent spec = template.getComponent();
        if (!SearchTemplateComponents.empty(spec)
            && spec.getMode() == SearchTemplateComponentMode.OWN_RUNG) {
          Map<String, String> config = SearchTemplateComponents.config(spec);
          SearchTemplateComponent implementation =
              coordinator.components().require(spec.getName(), spec.getVersion());
          SearchTemplateComponentPage ranked =
              implementation.execute(
                  SearchTemplateComponentRequest.builder()
                      .opContext(opContext)
                      .query(input)
                      .filter(postFilters)
                      .sort(sortCriteria)
                      .from(from)
                      .size(size)
                      .config(config)
                      .mode(spec.getMode())
                      .build());
          SearchResult owned = new SearchResult();
          owned.setEntities(new SearchEntityArray(ranked.hits()));
          owned.setNumEntities((int) Math.min(Integer.MAX_VALUE, Math.max(0, ranked.total())));
          owned.setFrom(from);
          owned.setPageSize(size == null ? ranked.hits().size() : size);
          if (ranked.metadata() != null) {
            owned.setMetadata(ranked.metadata());
          }
          return SearchTemplateWalk.AttemptResult.ok(
              owned,
              owned.getNumEntities(),
              new ArrayList<>(owned.getEntities()),
              List.of(),
              ranked.shardFailed(),
              null);
        }
        if (!SearchTemplateComponents.empty(spec)
            && spec.getMode() == SearchTemplateComponentMode.POST_PROCESS) {
          Map<String, String> config = SearchTemplateComponents.config(spec);
          SearchTemplateComponentRequest planned =
              SearchTemplateComponentRequest.builder()
                  .opContext(opContext)
                  .query(input)
                  .filter(postFilters)
                  .sort(sortCriteria)
                  .from(from)
                  .size(size)
                  .config(config)
                  .mode(spec.getMode())
                  .build();
          int window =
              coordinator
                  .components()
                  .require(spec.getName(), spec.getVersion())
                  .plan(planned, config);
          if (!SearchTemplateComponents.skip(false, size, sortCriteria, from, window)) {
            fetchFrom = 0;
            fetchSize = window;
          }
        }
      }
      SearchExecution<SearchResult> execution =
          searchOnceExecution(
              opContext,
              entityNames,
              input,
              postFilters,
              sortCriteria,
              fetchFrom,
              fetchSize,
              facets,
              urn);
      SearchResult page = execution.page();
      List<SearchEntity> entities =
          page.getEntities() == null ? List.of() : new ArrayList<>(page.getEntities());
      String rendered =
          execution.renderedTemplate() == null
              ? null
              : execution.renderedTemplate().query().toString();
      return SearchTemplateWalk.AttemptResult.ok(
          page,
          page.getNumEntities(),
          entities,
          execution.renderedTemplate() == null
              ? List.of()
              : execution.renderedTemplate().highlights(),
          execution.shardFailed(),
          rendered);
    } catch (RuntimeException e) {
      return SearchTemplateWalk.AttemptResult.failed(e);
    }
  }

  @Nonnull
  private SearchTemplateWalk.AttemptResult<?> componentReorder(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull SearchTemplateWalk.AttemptResult<?> raw,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    if (!(raw.page() instanceof SearchResult page)) {
      return raw;
    }
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    if (coordinator == null) {
      return raw;
    }
    MergedSearchTemplate template = coordinator.loader().load(opContext, urn);
    com.linkedin.search.template.SearchTemplateComponent spec = template.getComponent();
    if (SearchTemplateComponents.empty(spec) || spec.getMode() == null) {
      return raw;
    }
    @SuppressWarnings("unchecked")
    SearchTemplateWalk.AttemptResult<SearchResult> typed =
        (SearchTemplateWalk.AttemptResult<SearchResult>) raw;
    List<SearchEntity> hits =
        page.getEntities() == null ? List.of() : new ArrayList<>(page.getEntities());
    return SearchTemplateComponents.reorder(
        coordinator.components(),
        template,
        SearchTemplateComponentRequest.builder()
            .opContext(opContext)
            .query(input)
            .renderedQuery(typed.renderedQuery())
            .filter(postFilters)
            .sort(sortCriteria)
            .from(from)
            .size(size)
            .hits(hits)
            .total(page.getNumEntities())
            .metadata(page.getMetadata())
            .config(SearchTemplateComponents.config(spec))
            .mode(spec.getMode())
            .build(),
        typed);
  }

  @Nonnull
  SearchResult searchOnce(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return searchOnceExecution(
            opContext, entityNames, input, postFilters, sortCriteria, from, size, facets, null)
        .page();
  }

  @Nonnull
  private SearchExecution<SearchResult> searchOnceExecution(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets,
      @Nullable String renderUrn) {
    PreparedRequestComponents components =
        prepareTemplateSearch(
            opContext,
            entityNames,
            input,
            postFilters,
            sortCriteria,
            from,
            size,
            facets,
            renderUrn);
    return executeTemplateSearch(
        opContext,
        components.entitySpecs(),
        components.prepared().request(),
        components.filter(),
        from,
        size,
        components.prepared().template());
  }

  @Nonnull
  ScrollResult scrollWithTemplates(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size) {
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    ResolvedSearchTemplate resolved = opContext.getSearchContext().getResolvedSearchTemplate();
    if (coordinator != null && resolved != null) {
      SearchExecution<ScrollResult> execution =
          scrollOnceExecution(
              opContext,
              entities,
              input,
              postFilters,
              sortCriteria,
              scrollId,
              keepAlive,
              size,
              resolved.getUrn());
      if (execution.renderedTemplate() != null) {
        resolved.setHighlightFields(execution.renderedTemplate().highlights());
      }
      return execution.page();
    }
    return scrollOnceExecution(
            opContext, entities, input, postFilters, sortCriteria, scrollId, keepAlive, size, null)
        .page();
  }

  @Nonnull
  private SearchExecution<ScrollResult> scrollOnceExecution(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nullable String renderUrn) {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    final String finalInput = input.isEmpty() ? "*" : input;
    List<EntitySpec> entitySpecs =
        entities.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .distinct()
            .collect(Collectors.toList());
    String[] indexArray = dao.entityIndexNames(opContext, entities);
    Filter transformedFilters = dao.transformFilter(opContext, postFilters, indexConvention);
    boolean hasSliceOptions = opContext.getSearchContext().getSearchFlags().hasSliceOptions();
    boolean usePIT = (dao.pointInTimeCreationEnabled || hasSliceOptions) && keepAlive != null;
    String pitId =
        usePIT
            ? ESUtils.computePointInTime(
                opContext, scrollId, keepAlive, dao.searchClient(opContext, indexArray), indexArray)
            : null;
    Object[] sort = scrollId != null ? SearchAfterWrapper.fromScrollId(scrollId).getSort() : null;
    PreparedSearchRequest prepared =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                dao.searchConfiguration,
                dao.customSearchConfiguration,
                dao.queryFilterRewriteChain,
                dao.searchServiceConfig)
            .prepareSearchRequest(
                opContext,
                finalInput,
                transformedFilters,
                sortCriteria,
                sort,
                pitId,
                keepAlive,
                size,
                List.of(),
                renderUrn,
                true);
    if (!usePIT) {
      prepared.request().indices(indexArray);
    }
    return executeTemplateScroll(
        opContext,
        entitySpecs,
        prepared.request(),
        transformedFilters,
        keepAlive,
        size,
        prepared.template());
  }

  @Nonnull
  PreparedRequestComponents prepareTemplateSearch(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets,
      @Nullable String renderUrn) {
    final String finalInput = input.isEmpty() ? "*" : input;
    List<EntitySpec> entitySpecs =
        entityNames.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .distinct()
            .collect(Collectors.toList());
    Filter transformedFilters =
        dao.transformFilter(
            opContext, postFilters, opContext.getSearchContext().getIndexConvention());
    PreparedSearchRequest prepared =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                dao.searchConfiguration,
                dao.customSearchConfiguration,
                dao.queryFilterRewriteChain,
                dao.searchServiceConfig)
            .prepareSearchRequest(
                opContext,
                finalInput,
                transformedFilters,
                sortCriteria,
                from,
                size,
                facets,
                renderUrn,
                false);
    prepared.request().indices(dao.entityIndexNames(opContext, entityNames));
    return new PreparedRequestComponents(prepared, transformedFilters, entitySpecs);
  }

  @Nonnull
  private SearchExecution<SearchResult> executeTemplateSearch(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      int from,
      @Nullable Integer size,
      @Nullable RenderedSearchTemplate template) {
    SearchResponse searchResponse = null;
    try {
      searchResponse =
          dao.searchClient(opContext, searchRequest)
              .search(opContext, searchRequest, RequestOptions.DEFAULT);
      boolean shardFailed = searchResponse.getFailedShards() > 0 || searchResponse.isTimedOut();
      SearchResult page =
          dao.transformIndexIntoEntityName(
              opContext,
              opContext.getSearchContext().getIndexConvention(),
              SearchRequestHandler.getBuilder(
                      opContext,
                      entitySpecs,
                      dao.searchConfiguration,
                      dao.customSearchConfiguration,
                      dao.queryFilterRewriteChain,
                      dao.searchServiceConfig)
                  .extractResult(
                      opContext,
                      searchResponse,
                      filter,
                      from,
                      ConfigUtils.applyLimit(dao.searchServiceConfig, size),
                      template));
      return new SearchExecution<>(page, shardFailed, template);
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Nonnull
  private SearchExecution<ScrollResult> executeTemplateScroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nullable RenderedSearchTemplate template) {
    try {
      SearchResponse searchResponse =
          dao.searchClient(opContext, searchRequest)
              .search(opContext, searchRequest, RequestOptions.DEFAULT);
      ScrollResult page =
          dao.transformIndexIntoEntityName(
              opContext,
              opContext.getSearchContext().getIndexConvention(),
              SearchRequestHandler.getBuilder(
                      opContext,
                      entitySpecs,
                      dao.searchConfiguration,
                      dao.customSearchConfiguration,
                      dao.queryFilterRewriteChain,
                      dao.searchServiceConfig)
                  .extractScrollResult(
                      opContext,
                      searchResponse,
                      filter,
                      keepAlive,
                      size,
                      dao.pointInTimeCreationEnabled,
                      template));
      return new SearchExecution<>(
          page, searchResponse.getFailedShards() > 0 || searchResponse.isTimedOut(), template);
    } catch (Exception e) {
      log.error("Search query failed: {}", searchRequest, e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  boolean templateReads(@Nonnull OperationContext opContext) {
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    return coordinator != null && coordinator.executes(opContext);
  }

  /**
   * Renders the template for a path whose Java request is not built by {@code SearchQueryBuilder}.
   * The rendered body is not attached to the request.
   */
  void shadowTemplates(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String query,
      boolean scroll) {
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    if (coordinator == null
        || entityNames.isEmpty()
        || coordinator.effect(opContext) != SearchTemplateRollout.Effect.SHADOW) {
      return;
    }
    List<EntitySpec> entitySpecs = new ArrayList<>();
    for (String entityName : entityNames) {
      entitySpecs.add(opContext.getEntityRegistry().getEntitySpec(entityName));
    }
    boolean fulltext =
        opContext.getSearchContext().getSearchFlags() != null
            && Boolean.TRUE.equals(opContext.getSearchContext().getSearchFlags().isFulltext());
    coordinator.shadow(opContext, entitySpecs, query, fulltext, scroll);
  }

  @Nonnull
  QueryBuilder renderedStarQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter) {
    return prepareTemplateSearch(opContext, entityNames, "*", filter, null, 0, 0, List.of(), null)
        .prepared()
        .request()
        .source()
        .query();
  }
}
