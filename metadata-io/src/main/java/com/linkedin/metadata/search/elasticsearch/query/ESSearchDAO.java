package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.X_CONTENT_REGISTRY;
import static com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder.URN_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.RescoreFormulaConfig;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestOptions;
import com.linkedin.metadata.search.rescore.Exp4jRescorer;
import com.linkedin.metadata.search.rescore.NormalizationConfig;
import com.linkedin.metadata.search.rescore.NormalizationType;
import com.linkedin.metadata.search.rescore.RescoreResult;
import com.linkedin.metadata.search.rescore.SignalDefinition;
import com.linkedin.metadata.search.rescore.SignalType;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
@RequiredArgsConstructor
@Accessors(chain = true)
public class ESSearchDAO {

  private final SearchClientShim<?> client;
  private final boolean pointInTimeCreationEnabled;
  @Nonnull private final ElasticSearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final boolean testLoggingEnabled;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;

  /** Cached exp4j rescorer, lazily initialized */
  private final AtomicReference<Exp4jRescorer> cachedRescorer = new AtomicReference<>();

  /** Cached resolved rescore formula config */
  private final AtomicReference<RescoreFormulaConfig> resolvedRescoreFormulaConfig =
      new AtomicReference<>();

  /** ObjectMapper for JSON serialization of rescore explanations */
  private static final ObjectMapper RESCORE_MAPPER = new ObjectMapper();

  /** ObjectMapper for YAML parsing of rescore config */
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  public ESSearchDAO(
      SearchClientShim<?> client,
      boolean pointInTimeCreationEnabled,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchServiceConfiguration searchServiceConfig) {
    this(
        client,
        pointInTimeCreationEnabled,
        searchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        false,
        searchServiceConfig);
  }

  public long docCount(@Nonnull OperationContext opContext, @Nonnull String entityName) {
    return docCount(opContext, entityName, null);
  }

  public long docCount(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    CountRequest countRequest =
        new CountRequest(opContext.getSearchContext().getIndexConvention().getIndexName(entitySpec))
            .query(
                SearchRequestHandler.getFilterQuery(
                    opContext,
                    List.of(entityName),
                    filter,
                    entitySpec.getSearchableFieldTypes(),
                    queryFilterRewriteChain));

    return opContext.withSpan(
        "docCount",
        () -> {
          try {
            return client.count(countRequest, RequestOptions.DEFAULT).getCount();
          } catch (IOException e) {
            log.error("Count query failed:" + e.getMessage());
            throw new ESQueryException("Count query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "docCount"));
  }

  @Nonnull
  @WithSpan
  private SearchResult executeAndExtract(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      int from,
      @Nullable Integer size) {
    long id = System.currentTimeMillis();

    return opContext.withSpan(
        "executeAndExtract_search",
        () -> {
          try {
            if (resolveSearchV2_5Enabled(opContext)) {
              return executeAndExtractSearchV2_5(
                  opContext, entitySpec, searchRequest, filter, from, size, id);
            } else {
              return executeAndExtractSearchV2(
                  opContext, entitySpec, searchRequest, filter, from, size, id);
            }
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          } finally {
            log.debug("Returning from request {}.", id);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "executeAndExtract_search"));
  }

  /** V2 (control): Execute search as-is with no modification (acryl-main behavior). */
  private SearchResult executeAndExtractSearchV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      int from,
      @Nullable Integer size,
      long id)
      throws IOException {
    log.debug("Executing V2 request {} (no rescoring): {}", id, searchRequest);
    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

    try {
      return transformIndexIntoEntityName(
          opContext.getSearchContext().getIndexConvention(),
          SearchRequestHandler.getBuilder(
                  opContext,
                  entitySpec,
                  searchConfiguration,
                  customSearchConfiguration,
                  queryFilterRewriteChain,
                  searchServiceConfig)
              .extractResult(
                  opContext,
                  searchResponse,
                  filter,
                  from,
                  ConfigUtils.applyLimit(searchServiceConfig, size)));
    } catch (Exception e) {
      log.error("Response to the failed search query: {}", searchResponse);
      throw e;
    }
  }

  /** V2.5 (treatment): Fetch a rescore window and apply Stage 2 rescoring. */
  private SearchResult executeAndExtractSearchV2_5(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      int from,
      @Nullable Integer size,
      long id)
      throws IOException {
    RescoreFormulaConfig rescoreConfig = getRescoreFormulaConfig();
    int configuredWindow = rescoreConfig != null ? rescoreConfig.getWindowSize() : 100;
    int maxWindow = rescoreConfig != null ? rescoreConfig.getMaxRescoreWindow() : 5000;

    // Optimization: Skip rescoring if request is entirely beyond rescore window
    // Items at positions >= windowSize never get rescored, so we can fetch directly
    if (from >= configuredWindow) {
      log.debug(
          "Executing request {} without rescoring (from={} >= windowSize={}): {}",
          id,
          from,
          configuredWindow,
          searchRequest);
      return executeAndExtractSearchV2(
          opContext, entitySpec, searchRequest, filter, from, size, id);
    }

    // Calculate how many results to fetch for rescoring
    int rescoreWindow = calculateRescoreWindow(from, size, configuredWindow, maxWindow);

    // Store original pagination params
    int requestedFrom = from;
    Integer requestedSize = size;

    // Modify search request to fetch rescore window from beginning
    searchRequest.source().from(0).size(rescoreWindow);

    log.debug(
        "Executing request {}: {} (rescore window: {}, requested: from={}, size={})",
        id,
        searchRequest,
        rescoreWindow,
        requestedFrom,
        requestedSize);
    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

    // Extract results (full window)
    SearchResult fullWindowResult =
        transformIndexIntoEntityName(
            opContext.getSearchContext().getIndexConvention(),
            SearchRequestHandler.getBuilder(
                    opContext,
                    entitySpec,
                    searchConfiguration,
                    customSearchConfiguration,
                    queryFilterRewriteChain,
                    searchServiceConfig)
                .extractResult(
                    opContext,
                    searchResponse,
                    filter,
                    0, // Extract from beginning
                    rescoreWindow)); // Extract full window

    // Apply Stage 2 rescoring and slice to requested pagination
    SearchResult rescored = applyJavaRescore(opContext, searchResponse, fullWindowResult);
    return sliceSearchResultForPagination(rescored, requestedFrom, requestedSize);
  }

  private String transformIndexToken(
      IndexConvention indexConvention, String name, int entityTypeIdx) {
    if (entityTypeIdx < 0) {
      return name;
    }
    String[] tokens = name.split(AGGREGATION_SEPARATOR_CHAR);
    if (entityTypeIdx < tokens.length) {
      tokens[entityTypeIdx] =
          indexConvention.getEntityName(tokens[entityTypeIdx]).orElse(tokens[entityTypeIdx]);
    }
    return String.join(AGGREGATION_SEPARATOR_CHAR, tokens);
  }

  private AggregationMetadata transformAggregationMetadata(
      @Nonnull IndexConvention indexConvention,
      @Nonnull AggregationMetadata aggMeta,
      int entityTypeIdx) {
    if (entityTypeIdx >= 0) {
      aggMeta.setAggregations(
          new LongMap(
              aggMeta.getAggregations().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          entry ->
                              transformIndexToken(indexConvention, entry.getKey(), entityTypeIdx),
                          Map.Entry::getValue))));
      aggMeta.setFilterValues(
          new FilterValueArray(
              aggMeta.getFilterValues().stream()
                  .map(
                      filterValue ->
                          filterValue.setValue(
                              transformIndexToken(
                                  indexConvention, filterValue.getValue(), entityTypeIdx)))
                  .collect(Collectors.toList())));
    }
    return aggMeta;
  }

  private ScrollResult buildScrollResult(
      @Nonnull SearchResult searchResult, @Nullable String scrollId) {
    ScrollResult result =
        new ScrollResult()
            .setEntities(searchResult.getEntities())
            .setMetadata(searchResult.getMetadata())
            .setNumEntities(searchResult.getNumEntities())
            .setPageSize(searchResult.getPageSize());
    if (scrollId != null) {
      result.setScrollId(scrollId);
    }
    return result;
  }

  @VisibleForTesting
  public SearchResult transformIndexIntoEntityName(
      IndexConvention indexConvention, SearchResult result) {
    return result.setMetadata(
        result
            .getMetadata()
            .setAggregations(
                transformIndexIntoEntityName(
                    indexConvention, result.getMetadata().getAggregations())));
  }

  private ScrollResult transformIndexIntoEntityName(
      IndexConvention indexConvention, ScrollResult result) {
    return result.setMetadata(
        result
            .getMetadata()
            .setAggregations(
                transformIndexIntoEntityName(
                    indexConvention, result.getMetadata().getAggregations())));
  }

  private AggregationMetadataArray transformIndexIntoEntityName(
      @Nonnull IndexConvention indexConvention, AggregationMetadataArray aggArray) {
    List<AggregationMetadata> newAggs = new ArrayList<>();
    for (AggregationMetadata aggMeta : aggArray) {
      List<String> aggregateFacets = List.of(aggMeta.getName().split(AGGREGATION_SEPARATOR_CHAR));
      int entityTypeIdx = aggregateFacets.indexOf(INDEX_VIRTUAL_FIELD);
      newAggs.add(transformAggregationMetadata(indexConvention, aggMeta, entityTypeIdx));
    }
    return new AggregationMetadataArray(newAggs);
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeSearchScrollRequestAndExtract(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nullable Filter filters,
      @Nonnull SearchRequest searchRequest,
      @Nullable String keepAlive,
      @Nullable Integer size) {
    try {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      ScrollResult result =
          SearchRequestHandler.getBuilder(
                  opContext,
                  entitySpecs,
                  searchConfiguration,
                  customSearchConfiguration,
                  queryFilterRewriteChain,
                  searchServiceConfig)
              .extractScrollResult(
                  opContext, searchResponse, filters, keepAlive, size, pointInTimeCreationEnabled);

      // Apply Java-based rescoring (Stage 2) only for V2.5
      if (resolveSearchV2_5Enabled(opContext)) {
        return applyJavaRescore(opContext, searchResponse, result);
      } else {
        return result; // V2: Return ES results directly
      }
    } catch (Exception e) {
      log.error("Search Scroll query failed", e);
      throw new ESQueryException("Search Scroll query failed:", e);
    }
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeScrollRequestAndExtract(
      @Nonnull OperationContext opContext,
      @Nonnull EntitySpec entitySpec,
      @Nullable Filter filters,
      @Nonnull SearchScrollRequest searchScrollRequest,
      @Nullable Integer size,
      @Nonnull String keepAlive) {
    try {
      final SearchResponse searchResponse =
          client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      ScrollResult result =
          SearchRequestHandler.getBuilder(
                  opContext,
                  entitySpec,
                  searchConfiguration,
                  customSearchConfiguration,
                  queryFilterRewriteChain,
                  searchServiceConfig)
              .extractScrollResult(
                  opContext, searchResponse, filters, keepAlive, size, pointInTimeCreationEnabled);

      // Apply Java-based rescoring (Stage 2) only for V2.5
      if (resolveSearchV2_5Enabled(opContext)) {
        return applyJavaRescore(opContext, searchResponse, result);
      } else {
        return result; // V2: Return ES results directly
      }
    } catch (Exception e) {
      log.error("Scroll query failed", e);
      throw new ESQueryException("Scroll query failed:", e);
    }
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeAndExtract(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      @Nullable String keepAlive,
      @Nullable Integer size) {
    return opContext.withSpan(
        "executeAndExtract_scroll",
        () -> {
          try {
            final SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);
            // NOTE: Scroll-based search uses searchAfter pagination which is incompatible
            // with global rescore windowing. Rescoring is applied per-page for scroll,
            // which is acceptable since scroll is primarily used for bulk exports where
            // ranking consistency across page sizes is not critical.
            // extract results, validated against document model as well
            ScrollResult result =
                transformIndexIntoEntityName(
                    opContext.getSearchContext().getIndexConvention(),
                    SearchRequestHandler.getBuilder(
                            opContext,
                            entitySpecs,
                            searchConfiguration,
                            customSearchConfiguration,
                            queryFilterRewriteChain,
                            searchServiceConfig)
                        .extractScrollResult(
                            opContext,
                            searchResponse,
                            filter,
                            keepAlive,
                            ConfigUtils.applyLimit(searchServiceConfig, size),
                            pointInTimeCreationEnabled));

            // Apply Java-based rescoring (Stage 2) only for V2.5
            if (resolveSearchV2_5Enabled(opContext)) {
              return applyJavaRescore(opContext, searchResponse, result);
            } else {
              return result; // V2: Return ES results directly
            }
          } catch (Exception e) {
            log.error("Search query failed: {}", searchRequest, e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "executeAndExtract_scroll"));
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {

    // Step 1: construct the query
    final Triple<SearchRequest, Filter, List<EntitySpec>> searchRequestComponents =
        opContext.withSpan(
            "searchRequest",
            () ->
                buildSearchRequest(
                    opContext, entityNames, input, postFilters, sortCriteria, from, size, facets),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(this.getClass(), "searchRequest"));

    if (testLoggingEnabled) {
      testLog(opContext.getObjectMapper(), searchRequestComponents.getLeft());
    }

    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(
        opContext,
        searchRequestComponents.getRight(),
        searchRequestComponents.getLeft(),
        searchRequestComponents.getMiddle(),
        from,
        size);
  }

  @VisibleForTesting
  public Triple<SearchRequest, Filter, List<EntitySpec>> buildSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {

    final String finalInput = input.isEmpty() ? "*" : input;

    List<EntitySpec> entitySpecs =
        entityNames.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .distinct()
            .collect(Collectors.toList());
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);

    SearchDocFieldFetchConfig searchDocFieldFetchConfig = null;
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    if (searchFlags != null && searchFlags.getFetchExtraFields() != null) {
      Set<String> allFieldsToFetch =
          new HashSet<>(SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH);
      allFieldsToFetch.addAll(searchFlags.getFetchExtraFields());
      searchDocFieldFetchConfig = new SearchDocFieldFetchConfig().fieldsToFetch(allFieldsToFetch);
    }

    SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getSearchRequest(
                opContext,
                SearchRequestOptions.builder()
                    .input(finalInput)
                    .filter(transformedFilters)
                    .sortCriteria(sortCriteria)
                    .from(from)
                    .size(size)
                    .facets(facets)
                    .fieldFetchConfig(searchDocFieldFetchConfig)
                    .build())
            .indices(
                entityNames.stream()
                    .map(indexConvention::getEntityIndexName)
                    .toArray(String[]::new));

    return Triple.of(searchRequest, transformedFilters, entitySpecs);
  }

  /**
   * Gets a list of documents after applying the input filters.
   *
   * @param filters the request map with fields and values to be applied as filters to the search
   *     query
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size number of search hits to return
   * @return a {@link SearchResult} that contains a list of filtered documents and related search
   *     result metadata
   */
  @Nonnull
  public SearchResult filter(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    Filter transformedFilters = transformFilterForEntities(filters, indexConvention);
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getFilterRequest(opContext, transformedFilters, sortCriteria, from, size);

    searchRequest.indices(indexConvention.getIndexName(entitySpec));
    return executeAndExtract(
        opContext, List.of(entitySpec), searchRequest, transformedFilters, from, size);
  }

  /**
   * Scroll through documents that matches the input filters. By using the returned scroll ID, we
   * can scroll through unlimited number of documents that match the input filters. HOWEVER, this is
   * very resource intensive and is not meant for real-time queries
   *
   * @param entities name of the entity
   * @param filters the request map with fields and values to be applied as filters to the search
   *     query
   * @param sortCriteria {@link SortCriterion} to be applied to search results
   * @param size number of search hits to return
   * @param scrollId Unique ID corresponding to the search context. Set as null for the initial
   *     request and then set as the returned scroll ID to continue retrieving documents for the
   *     initial search context
   * @param keepAlive duration the search context should be kept alive i.e. 10s, 1m
   * @return a {@link ScrollResult} that contains a list of filtered documents and related search
   *     result metadata
   */
  @Nonnull
  @WithSpan
  public ScrollResult scroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nullable Filter filters,
      @Nullable List<SortCriterion> sortCriteria,
      @Nullable Integer size,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable SearchDocFieldFetchConfig searchDocFieldFetchConfig) {

    size = ConfigUtils.applyLimit(searchServiceConfig, size);

    List<EntitySpec> entitySpecs =
        entities.stream()
            .map(specName -> opContext.getEntityRegistry().getEntitySpec(specName))
            .collect(Collectors.toList());
    String[] indexArray =
        entities.stream()
            .map(
                entityName ->
                    opContext
                        .getSearchContext()
                        .getIndexConvention()
                        .getEntityIndexName(entityName))
            .toArray(String[]::new);

    // If scrollID is null, it is the initial scroll request -> execute search request with the
    // scroll setting
    boolean hasSliceOptions = opContext.getSearchContext().getSearchFlags().hasSliceOptions();
    if (hasSliceOptions && isSliceDisabled()) {
      throw new IllegalStateException(
          "Slice options are not supported with the current ES implementation: "
              + client.getEngineType()
              + ". Please disable slice options in the search flags.");
    }

    boolean usePIT = (pointInTimeCreationEnabled || hasSliceOptions) && keepAlive != null;
    String pitId =
        usePIT ? ESUtils.computePointInTime(scrollId, keepAlive, client, indexArray) : null;
    Object[] sort = scrollId != null ? SearchAfterWrapper.fromScrollId(scrollId).getSort() : null;

    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getSearchRequest(
                opContext,
                SearchRequestOptions.builder()
                    .filter(filters)
                    .sortCriteria(sortCriteria)
                    .size(size)
                    .keepAlive(keepAlive)
                    .pitId(pitId)
                    .searchAfter(sort)
                    .fieldFetchConfig(searchDocFieldFetchConfig)
                    .build());

    // PIT specifies indices in creation so it doesn't support specifying indices on the request, so
    // we only specify if not using PIT
    if (!usePIT) {
      searchRequest.indices(indexArray);
    }

    return executeSearchScrollRequestAndExtract(
        opContext, entitySpecs, filters, searchRequest, keepAlive, size);
  }

  /**
   * Returns a list of suggestions given type ahead query.
   *
   * <p>The advanced auto complete can take filters and provides suggestions based on filtered
   * context.
   *
   * @param query the type ahead query text
   * @param field the field name for the auto complete
   * @param requestParams specify the field to auto complete and the input text
   * @param limit the number of suggestions returned
   * @return A list of suggestions as string
   */
  @Nonnull
  public AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    try {
      Pair<SearchRequest, AutocompleteRequestHandler> searchRequestAndBuilder =
          buildAutocompleteRequest(opContext, entityName, query, field, requestParams, limit);
      SearchResponse searchResponse =
          client.search(searchRequestAndBuilder.getLeft(), RequestOptions.DEFAULT);
      return searchRequestAndBuilder.getRight().extractResult(opContext, searchResponse, query);
    } catch (Exception e) {
      log.error("Auto complete query failed:" + e.getMessage());
      throw new ESQueryException("Auto complete query failed:", e);
    }
  }

  @VisibleForTesting
  public Pair<SearchRequest, AutocompleteRequestHandler> buildAutocompleteRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    AutocompleteRequestHandler builder =
        AutocompleteRequestHandler.getBuilder(
            opContext,
            entitySpec,
            customSearchConfiguration,
            queryFilterRewriteChain,
            searchConfiguration,
            searchServiceConfig);
    SearchRequest req =
        builder.getSearchRequest(
            opContext,
            entityName,
            query,
            field,
            transformFilterForEntities(requestParams, indexConvention),
            limit);
    req.indices(indexConvention.getIndexName(entitySpec));
    return Pair.of(req, builder);
  }

  /**
   * Returns number of documents per field value given the field and filters
   *
   * @param entityNames names of the entities, if null, aggregates over all entities
   * @param field the field name for aggregate
   * @param requestParams filters to apply before aggregating
   * @param limit the number of aggregations to return
   * @return
   */
  @Nonnull
  public Map<String, Long> aggregateByValue(
      @Nonnull OperationContext opContext,
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {

    return opContext.withSpan(
        "aggregateByValue_search",
        () -> {
          try {
            final SearchRequest searchRequest =
                buildAggregateByValue(opContext, entityNames, field, requestParams, limit);
            final SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);
            // extract results, validated against document model as well
            return AggregationQueryBuilder.extractAggregationsFromResponse(searchResponse, field);
          } catch (Exception e) {
            log.error("Aggregation query failed", e);
            throw new ESQueryException("Aggregation query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "aggregateByValue_search"));
  }

  @VisibleForTesting
  public SearchRequest buildAggregateByValue(
      @Nonnull OperationContext opContext,
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    List<EntitySpec> entitySpecs;
    if (entityNames == null || entityNames.isEmpty()) {
      entitySpecs = QueryUtils.getQueryByDefaultEntitySpecs(opContext.getEntityRegistry());
    } else {
      entitySpecs =
          entityNames.stream()
              .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
              .distinct()
              .collect(Collectors.toList());
    }
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getAggregationRequest(
                opContext,
                field,
                transformFilterForEntities(requestParams, indexConvention),
                limit);
    if (entityNames == null) {
      List<String> indexPatterns = indexConvention.getAllEntityIndicesPatterns();
      searchRequest.indices(indexPatterns.toArray(new String[0]));
    } else {
      Stream<String> stream =
          entityNames.stream()
              .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
              .map(indexConvention::getIndexName);
      searchRequest.indices(stream.toArray(String[]::new));
    }
    return searchRequest;
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll Id to convert to a PIT ID and Sort array to pass to ElasticSearch
   * @param keepAlive string representation of the time to keep a point in time alive
   * @param size the number of search hits to return
   * @return a {@link ScrollResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public ScrollResult scroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size) {

    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();

    final Triple<SearchRequest, Filter, List<EntitySpec>> searchRequestAndSpecs =
        opContext.withSpan(
            "scrollRequest",
            () -> {
              // TODO: Align scroll and search using facets
              final Triple<SearchRequest, Filter, List<EntitySpec>> req =
                  buildScrollRequest(
                      opContext,
                      indexConvention,
                      scrollId,
                      keepAlive,
                      entities,
                      size,
                      postFilters,
                      input,
                      sortCriteria,
                      List.of());
              return req;
            },
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(this.getClass(), "scrollRequest"));

    if (testLoggingEnabled) {
      testLog(opContext.getObjectMapper(), searchRequestAndSpecs.getLeft());
    }

    return executeAndExtract(
        opContext,
        searchRequestAndSpecs.getRight(),
        searchRequestAndSpecs.getLeft(),
        searchRequestAndSpecs.getMiddle(),
        keepAlive,
        size);
  }

  @VisibleForTesting
  public Triple<SearchRequest, Filter, List<EntitySpec>> buildScrollRequest(
      @Nonnull OperationContext opContext,
      @Nonnull IndexConvention indexConvention,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nonnull List<String> entities,
      @Nullable Integer size,
      @Nullable Filter postFilters,
      String input,
      List<SortCriterion> sortCriteria,
      @Nonnull List<String> facets) {
    final String finalInput = input.isEmpty() ? "*" : input;

    List<EntitySpec> entitySpecs =
        entities.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .distinct()
            .collect(Collectors.toList());

    String[] indexArray =
        entities.stream().map(indexConvention::getEntityIndexName).toArray(String[]::new);

    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    boolean hasSliceOptions = opContext.getSearchContext().getSearchFlags().hasSliceOptions();
    if (hasSliceOptions && isSliceDisabled()) {
      throw new IllegalStateException(
          "Slice options are not supported with the current ES implementation: "
              + client.getEngineType()
              + ". Please disable slice options in the search flags.");
    }

    boolean usePIT = (pointInTimeCreationEnabled || hasSliceOptions) && keepAlive != null;
    String pitId =
        usePIT ? ESUtils.computePointInTime(scrollId, keepAlive, client, indexArray) : null;
    Object[] sort = scrollId != null ? SearchAfterWrapper.fromScrollId(scrollId).getSort() : null;

    SearchDocFieldFetchConfig searchDocFieldFetchConfig = null;
    if (searchFlags.getFetchExtraFields() != null) {
      Set<String> allFieldsToFetch =
          new HashSet<>(SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL);
      allFieldsToFetch.addAll(searchFlags.getFetchExtraFields());
      searchDocFieldFetchConfig = new SearchDocFieldFetchConfig().fieldsToFetch(allFieldsToFetch);
    }
    SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getSearchRequest(
                opContext,
                SearchRequestOptions.builder()
                    .input(finalInput)
                    .filter(transformedFilters)
                    .sortCriteria(sortCriteria)
                    .searchAfter(sort)
                    .pitId(pitId)
                    .keepAlive(keepAlive)
                    .size(size)
                    .facets(facets)
                    .fieldFetchConfig(searchDocFieldFetchConfig)
                    .build());

    // PIT specifies indices in creation so it doesn't support specifying indices on the
    // request, so
    // we only specify if not using PIT
    if (!usePIT) {
      searchRequest.indices(indexArray);
    }

    return Triple.of(searchRequest, transformedFilters, entitySpecs);
  }

  public Optional<SearchResponse> raw(
      @Nonnull OperationContext opContext, @Nonnull String indexName, @Nullable String jsonQuery) {
    return Optional.ofNullable(jsonQuery)
        .map(
            json -> {
              try {
                XContentParser parser =
                    XContentType.JSON
                        .xContent()
                        .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json);
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);

                SearchRequest searchRequest =
                    new SearchRequest(
                        opContext.getSearchContext().getIndexConvention().getIndexName(indexName));
                searchRequest.source(searchSourceBuilder);

                return client.search(searchRequest, RequestOptions.DEFAULT);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  public Map<Urn, SearchResponse> rawEntity(@Nonnull OperationContext opContext, Set<Urn> urns) {
    EntityRegistry entityRegistry = opContext.getEntityRegistry();
    Map<Urn, EntitySpec> specs =
        urns.stream()
            .flatMap(
                urn ->
                    Optional.ofNullable(entityRegistry.getEntitySpec(urn.getEntityType()))
                        .map(spec -> Map.entry(urn, spec))
                        .stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return specs.entrySet().stream()
        .map(
            entry -> {
              try {
                String indexName =
                    opContext
                        .getSearchContext()
                        .getIndexConvention()
                        .getIndexName(entry.getValue());

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(
                    QueryBuilders.termQuery(URN_FIELD, entry.getKey().toString()));

                SearchRequest searchRequest = new SearchRequest(indexName);
                searchRequest.source(searchSourceBuilder);

                return Map.entry(
                    entry.getKey(), client.search(searchRequest, RequestOptions.DEFAULT));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private boolean isSliceDisabled() {
    return SearchClientShim.SearchEngineType.ELASTICSEARCH_7.equals(client.getEngineType());
  }

  public ExplainResponse explain(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nonnull String documentId,
      @Nonnull String entityName,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();

    final Triple<SearchRequest, Filter, List<EntitySpec>> searchRequest =
        buildScrollRequest(
            opContext,
            indexConvention,
            scrollId,
            keepAlive,
            List.of(entityName),
            size,
            postFilters,
            query,
            sortCriteria,
            facets);

    ExplainRequest explainRequest = new ExplainRequest();
    explainRequest
        .query(searchRequest.getLeft().source().query())
        .id(documentId)
        .index(indexConvention.getEntityIndexName(entityName));
    try {
      return client.explain(explainRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to explain query.", e);
      throw new IllegalStateException("Failed to explain query:", e);
    }
  }

  private void testLog(ObjectMapper mapper, SearchRequest searchRequest) {
    try {
      log.debug("SearchRequest(custom): {}", mapper.writeValueAsString(customSearchConfiguration));
      final String[] indices = searchRequest.indices();
      log.debug(
          String.format(
              "SearchRequest(indices): %s",
              mapper.writerWithDefaultPrettyPrinter().writeValueAsString(indices)));
      log.debug(
          String.format(
              "SearchRequest(query): %s",
              mapper.writeValueAsString(mapper.readTree(searchRequest.source().toString()))));
    } catch (JsonProcessingException e) {
      log.error("Error writing test log");
    }
  }

  // SAAS ONLY - Support predicate based filters
  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param predicateFilter the request map with fields and values as filters to be applied to
   *     search hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  @WithSpan
  public SearchResult predicateSearch(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Predicate predicateFilter,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    final String finalInput = input.isEmpty() ? "*" : input;

    List<EntitySpec> entitySpecs =
        entityNames.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .collect(Collectors.toList());
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    // Step 1: construct the query
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getPredicateSearchRequest(
                opContext, finalInput, predicateFilter, sortCriteria, from, size, facets);
    searchRequest.indices(
        entityNames.stream().map(indexConvention::getEntityIndexName).toArray(String[]::new));

    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtractPredicateSearch(
        opContext, entitySpecs, searchRequest, predicateFilter, from, size);
  }

  @Nonnull
  @WithSpan
  private SearchResult executeAndExtractPredicateSearch(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Predicate predicate,
      int from,
      @Nullable Integer size) {
    long id = System.currentTimeMillis();
    try {
      if (resolveSearchV2_5Enabled(opContext)) {
        return executeAndExtractPredicateSearchV2_5(
            opContext, entitySpec, searchRequest, predicate, from, size, id);
      } else {
        return executeAndExtractPredicateSearchV2(
            opContext, entitySpec, searchRequest, predicate, from, size, id);
      }
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    } finally {
      log.debug("Returning from request {}.", id);
    }
  }

  /** V2 (control): Execute predicate search as-is with no modification (acryl-main behavior). */
  private SearchResult executeAndExtractPredicateSearchV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Predicate predicate,
      int from,
      @Nullable Integer size,
      long id)
      throws IOException {
    log.debug("Executing V2 predicate search request {} (no rescoring): {}", id, searchRequest);
    final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

    return transformIndexIntoEntityName(
        opContext.getSearchContext().getIndexConvention(),
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .extractPredicateResult(opContext, searchResponse, predicate, from, size));
  }

  /** V2.5 (treatment): Fetch a rescore window and apply Stage 2 predicate rescoring. */
  private SearchResult executeAndExtractPredicateSearchV2_5(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Predicate predicate,
      int from,
      @Nullable Integer size,
      long id)
      throws IOException {
    RescoreFormulaConfig rescoreConfig = getRescoreFormulaConfig();
    int configuredWindow = rescoreConfig != null ? rescoreConfig.getWindowSize() : 100;
    int maxWindow = rescoreConfig != null ? rescoreConfig.getMaxRescoreWindow() : 5000;

    // Optimization: Skip rescoring if request is entirely beyond rescore window
    // Items at positions >= windowSize never get rescored, so we can fetch directly
    if (from >= configuredWindow) {
      log.debug(
          "Executing predicate search request {} without rescoring (from={} >= windowSize={}): {}",
          id,
          from,
          configuredWindow,
          searchRequest);
      return executeAndExtractPredicateSearchV2(
          opContext, entitySpec, searchRequest, predicate, from, size, id);
    }

    // Calculate how many results to fetch for rescoring
    int rescoreWindow = calculateRescoreWindow(from, size, configuredWindow, maxWindow);

    // Store original pagination params
    int requestedFrom = from;
    Integer requestedSize = size;

    // Modify search request to fetch rescore window from beginning
    searchRequest.source().from(0).size(rescoreWindow);

    log.debug(
        "Executing predicate search request {}: {} (rescore window: {}, requested: from={}, size={})",
        id,
        searchRequest,
        rescoreWindow,
        requestedFrom,
        requestedSize);
    final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

    // Extract results (full window)
    SearchResult fullWindowResult =
        transformIndexIntoEntityName(
            opContext.getSearchContext().getIndexConvention(),
            SearchRequestHandler.getBuilder(
                    opContext,
                    entitySpec,
                    searchConfiguration,
                    customSearchConfiguration,
                    queryFilterRewriteChain,
                    searchServiceConfig)
                .extractPredicateResult(opContext, searchResponse, predicate, 0, rescoreWindow));

    // Apply Stage 2 rescoring and slice to requested pagination
    SearchResult rescored = applyJavaRescore(opContext, searchResponse, fullWindowResult);
    return sliceSearchResultForPagination(rescored, requestedFrom, requestedSize);
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param predicate the predicate to filter on
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll Id to convert to a PIT ID and Sort array to pass to ElasticSearch
   * @param keepAlive string representation of the time to keep a point in time alive
   * @param size the number of search hits to return
   * @return a {@link ScrollResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @WithSpan
  @Nonnull
  public ScrollResult predicateScroll(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<String> entities,
      @Nonnull String input,
      @Nullable Predicate predicate,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size) {
    final String finalInput = input.isEmpty() ? "*" : input;
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String[] indexArray =
        entities.stream().map(indexConvention::getEntityIndexName).toArray(String[]::new);

    List<EntitySpec> entitySpecs =
        entities.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .collect(Collectors.toList());
    // TODO: Align scroll and search using facets
    final SearchRequest searchRequest =
        getPredicateScrollRequest(
            opContext,
            scrollId,
            keepAlive,
            indexArray,
            size,
            predicate,
            entitySpecs,
            finalInput,
            sortCriteria,
            Collections.emptyList());

    return executeAndExtractPredicateScroll(
        opContext, entitySpecs, searchRequest, predicate, keepAlive, size);
  }

  private SearchRequest getPredicateScrollRequest(
      @Nonnull OperationContext opContext,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      String[] indexArray,
      @Nullable Integer size,
      @Nullable Predicate predicate,
      List<EntitySpec> entitySpecs,
      String finalInput,
      List<SortCriterion> sortCriteria,
      @Nonnull List<String> facets) {
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();

    boolean hasSliceOptions = searchFlags.hasSliceOptions();
    if (hasSliceOptions && isSliceDisabled()) {
      throw new IllegalStateException(
          "Slice options are not supported with the current ES implementation: "
              + client.getEngineType()
              + ". Please disable slice options in the search flags.");
    }

    boolean usePIT = (pointInTimeCreationEnabled || hasSliceOptions) && keepAlive != null;
    String pitId =
        usePIT ? ESUtils.computePointInTime(scrollId, keepAlive, client, indexArray) : null;
    Object[] sort = scrollId != null ? SearchAfterWrapper.fromScrollId(scrollId).getSort() : null;

    SearchDocFieldFetchConfig searchDocFieldFetchConfig = null;
    if (searchFlags.getFetchExtraFields() != null) {
      Set<String> allFieldsToFetch =
          new HashSet<>(SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL);
      allFieldsToFetch.addAll(searchFlags.getFetchExtraFields());
      searchDocFieldFetchConfig = new SearchDocFieldFetchConfig().fieldsToFetch(allFieldsToFetch);
    }
    SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getPredicateSearchRequest(
                opContext,
                finalInput,
                predicate,
                sortCriteria,
                sort,
                pitId,
                keepAlive,
                size,
                facets,
                searchDocFieldFetchConfig);

    // PIT specifies indices in creation so it doesn't support specifying indices on the request, so
    // we only specify if not using PIT
    if (!usePIT) {
      searchRequest.indices(indexArray);
    }
    return searchRequest;
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeAndExtractPredicateScroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull SearchRequest searchRequest,
      @Nullable Predicate predicate,
      @Nullable String keepAlive,
      @Nullable Integer size) {
    try {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      ScrollResult result =
          transformIndexIntoEntityName(
              opContext.getSearchContext().getIndexConvention(),
              SearchRequestHandler.getBuilder(
                      opContext,
                      entitySpecs,
                      searchConfiguration,
                      customSearchConfiguration,
                      queryFilterRewriteChain,
                      searchServiceConfig)
                  .extractPredicateScrollResult(
                      opContext,
                      searchResponse,
                      predicate,
                      keepAlive,
                      size,
                      pointInTimeCreationEnabled));

      // Apply Java-based rescoring (Stage 2) only for V2.5
      if (resolveSearchV2_5Enabled(opContext)) {
        return applyJavaRescore(opContext, searchResponse, result);
      } else {
        return result; // V2: Return ES results directly
      }
    } catch (Exception e) {
      log.error("Search query failed: {}", searchRequest, e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  // ==================== Java/exp4j Rescoring ====================

  /**
   * Get or initialize the exp4j rescorer based on configuration.
   *
   * @return the rescorer, or null if rescoring is disabled
   */
  @Nullable
  private Exp4jRescorer getRescorer() {
    RescoreFormulaConfig config = getRescoreFormulaConfig();
    if (config == null || !config.isEnabled() || config.getFormula() == null) {
      return null;
    }

    return cachedRescorer.updateAndGet(
        existing -> {
          if (existing != null) {
            return existing;
          }
          try {
            List<SignalDefinition> signalDefs = buildSignalDefinitions(config);
            return new Exp4jRescorer(config.getFormula(), signalDefs);
          } catch (Exception e) {
            log.error("Failed to initialize Exp4jRescorer", e);
            return null;
          }
        });
  }

  /**
   * Get rescorer with UI overrides if provided, otherwise use server default.
   *
   * @param searchFlags SearchFlags containing potential overrides
   * @return the rescorer to use, or null if rescoring is disabled
   */
  @Nullable
  private Exp4jRescorer getRescorerWithOverrides(@Nullable SearchFlags searchFlags) {
    if (searchFlags == null) {
      return getRescorer();
    }

    // Check for formula or signal overrides
    boolean hasFormulaOverride = searchFlags.hasRescoreFormulaOverride();
    boolean hasSignalsOverride = searchFlags.hasRescoreSignalsOverride();

    if (!hasFormulaOverride && !hasSignalsOverride) {
      return getRescorer();
    }

    try {
      // Get base config
      RescoreFormulaConfig baseConfig = getRescoreFormulaConfig();
      if (baseConfig == null) {
        log.warn("Cannot apply rescore overrides: no base configuration found");
        return null;
      }

      // Use override formula if provided, otherwise use base config formula
      String formula =
          hasFormulaOverride ? searchFlags.getRescoreFormulaOverride() : baseConfig.getFormula();

      // Use override signals if provided, otherwise use base config signals
      List<SignalDefinition> signals;
      if (hasSignalsOverride) {
        signals = parseSignalOverrides(searchFlags.getRescoreSignalsOverride());
      } else {
        signals = buildSignalDefinitions(baseConfig);
      }

      return new Exp4jRescorer(formula, signals);
    } catch (Exception e) {
      log.warn(
          "Failed to build override rescorer: {}. Falling back to server default.", e.getMessage());
      return getRescorer();
    }
  }

  /**
   * Parse signal overrides from JSON string.
   *
   * @param signalsJson JSON array of signal definitions
   * @return list of signal definitions
   */
  private List<SignalDefinition> parseSignalOverrides(String signalsJson) throws Exception {
    List<Map<String, Object>> signalMaps =
        RESCORE_MAPPER.readValue(
            signalsJson, new com.fasterxml.jackson.core.type.TypeReference<>() {});

    List<SignalDefinition> signals = new ArrayList<>();
    for (Map<String, Object> signalMap : signalMaps) {
      // Validate required fields
      String name = (String) signalMap.get("name");
      String normalizedName = (String) signalMap.get("normalizedName");
      String fieldPath = (String) signalMap.get("fieldPath");

      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Signal 'name' is required");
      }
      if (normalizedName == null || normalizedName.isEmpty()) {
        throw new IllegalArgumentException("Signal 'normalizedName' is required for: " + name);
      }
      if (fieldPath == null || fieldPath.isEmpty()) {
        throw new IllegalArgumentException("Signal 'fieldPath' is required for: " + name);
      }

      // Default type to NUMERIC if not specified
      String typeStr = signalMap.containsKey("type") ? signalMap.get("type").toString() : "NUMERIC";
      SignalType signalType = SignalType.valueOf(typeStr.toUpperCase().replace("-", "_"));

      NormalizationConfig normConfig = NormalizationConfig.none();
      @SuppressWarnings("unchecked")
      Map<String, Object> normMap = (Map<String, Object>) signalMap.get("normalization");
      if (normMap != null) {
        // Default normalization type to NONE if not specified
        String normTypeStr = normMap.containsKey("type") ? normMap.get("type").toString() : "NONE";
        NormalizationType normType =
            NormalizationType.valueOf(normTypeStr.toUpperCase().replace("-", "_"));
        // Use same defaults as NormalizationYaml for consistency
        normConfig =
            NormalizationConfig.builder()
                .type(normType)
                .inputMin(getDoubleValue(normMap, "inputMin", 0.0))
                .inputMax(getDoubleValue(normMap, "inputMax", 1000.0))
                .steepness(getDoubleValue(normMap, "steepness", 6.0)) // Match YAML default
                .scale(getDoubleValue(normMap, "scale", 180.0)) // Match YAML default
                .outputMin(getDoubleValue(normMap, "outputMin", 1.0))
                .outputMax(getDoubleValue(normMap, "outputMax", 2.0))
                .trueValue(getDoubleValue(normMap, "trueValue", 1.0)) // Match YAML default
                .falseValue(getDoubleValue(normMap, "falseValue", 1.0))
                .build();
      }

      Map<String, Double> entityTypeBoosts = Collections.emptyMap();
      @SuppressWarnings("unchecked")
      Map<String, Object> boostsMap = (Map<String, Object>) signalMap.get("entityTypeBoosts");
      if (boostsMap != null) {
        entityTypeBoosts = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : boostsMap.entrySet()) {
          entityTypeBoosts.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
        }
      }

      signals.add(
          SignalDefinition.builder()
              .name(name)
              .normalizedName(normalizedName)
              .fieldPath(fieldPath)
              .type(signalType)
              .normalization(normConfig)
              .boost(getDoubleValue(signalMap, "boost", 1.0))
              .entityTypeBoosts(entityTypeBoosts)
              .build());
    }
    return signals;
  }

  private double getDoubleValue(Map<String, Object> map, String key, double defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return Double.parseDouble(String.valueOf(value));
  }

  @Nullable
  private RescoreFormulaConfig getRescoreFormulaConfig() {
    // Return cached resolved config if available
    RescoreFormulaConfig cached = resolvedRescoreFormulaConfig.get();
    if (cached != null) {
      return cached;
    }

    if (searchConfiguration.getSearch() == null) {
      return null;
    }

    RescoreFormulaConfig config = searchConfiguration.getSearch().getRescoreFormula();
    if (config == null) {
      return null;
    }

    // Resolve config from YAML file if enabled and formula not already loaded
    if (config.isEnabled() && config.getFormula() == null && config.getFile() != null) {
      try {
        config = config.resolve(YAML_MAPPER);
        resolvedRescoreFormulaConfig.set(config);
        log.info(
            "Resolved rescore formula config: enabled={}, formula={}",
            config.isEnabled(),
            config.getFormula() != null
                ? config.getFormula().substring(0, Math.min(50, config.getFormula().length()))
                    + "..."
                : null);
      } catch (IOException e) {
        log.warn("Failed to resolve rescore formula config from file: {}", e.getMessage());
      }
    } else if (config.getFormula() != null) {
      // Config already has formula (e.g., set directly), cache it
      resolvedRescoreFormulaConfig.set(config);
    }

    return config;
  }

  private List<SignalDefinition> buildSignalDefinitions(RescoreFormulaConfig config) {
    List<SignalDefinition> defs = new ArrayList<>();
    if (config.getSignals() == null) {
      return defs;
    }

    for (RescoreFormulaConfig.SignalConfig signalConfig : config.getSignals()) {
      SignalType signalType =
          SignalType.valueOf(signalConfig.getType().toUpperCase().replace("-", "_"));

      NormalizationConfig normConfig = buildNormalizationConfig(signalConfig.getNormalization());

      defs.add(
          SignalDefinition.builder()
              .name(signalConfig.getName())
              .normalizedName(signalConfig.getNormalizedName())
              .fieldPath(signalConfig.getFieldPath())
              .type(signalType)
              .normalization(normConfig)
              .boost(signalConfig.getBoost())
              .entityTypeBoosts(signalConfig.getEntityTypeBoosts())
              .build());
    }
    return defs;
  }

  private NormalizationConfig buildNormalizationConfig(
      RescoreFormulaConfig.NormalizationYaml normYaml) {
    if (normYaml == null) {
      return NormalizationConfig.none();
    }

    NormalizationType normType =
        NormalizationType.valueOf(normYaml.getType().toUpperCase().replace("-", "_"));

    return NormalizationConfig.builder()
        .type(normType)
        .inputMin(normYaml.getInputMin())
        .inputMax(normYaml.getInputMax())
        .steepness(normYaml.getSteepness())
        .scale(normYaml.getScale())
        .outputMin(normYaml.getOutputMin())
        .outputMax(normYaml.getOutputMax())
        .trueValue(normYaml.getTrueValue())
        .falseValue(normYaml.getFalseValue())
        .build();
  }

  /**
   * Apply Java-based rescoring to search results.
   *
   * @param opContext operation context
   * @param searchResponse raw ES response containing SearchHits
   * @param result transformed search result
   * @return rescored search result with updated scores and explanations
   */
  private SearchResult applyJavaRescore(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nonnull SearchResult result) {

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();

    // Check if rescoring is explicitly disabled via SearchFlags
    if (searchFlags != null && searchFlags.hasRescoreEnabled() && !searchFlags.isRescoreEnabled()) {
      log.debug("Rescoring explicitly disabled via searchFlags");
      return result;
    }

    // Get rescorer - use override if provided, otherwise use server default
    Exp4jRescorer rescorer = getRescorerWithOverrides(searchFlags);
    if (rescorer == null) {
      log.debug("Rescorer is null - rescoring skipped");
      return result;
    }

    RescoreFormulaConfig config = getRescoreFormulaConfig();

    // Check SearchFlags.includeExplain or config.includeExplain
    boolean includeExplain =
        (searchFlags != null && searchFlags.hasIncludeExplain() && searchFlags.isIncludeExplain())
            || (config != null && config.isIncludeExplain());

    // Build a map from document ID to SearchHit for efficient lookup
    Map<String, SearchHit> hitMap = new HashMap<>();
    for (SearchHit hit : searchResponse.getHits().getHits()) {
      hitMap.put(hit.getId(), hit);
    }

    // Get window size - only rescore top K entities
    int windowSize = config != null ? config.getWindowSize() : 100;
    List<SearchEntity> allEntities = result.getEntities();

    // Guard against null or empty entity list
    if (allEntities == null || allEntities.isEmpty()) {
      return result;
    }

    int totalEntities = allEntities.size();
    int actualWindowSize = Math.min(windowSize, totalEntities);

    // Dynamic boost calculation using recognizable tier values.
    // We use round numbers (1, 10, 100, 1000, ...) as boost values to make scores
    // easily interpretable:
    //   - If item #100's original BM25 is 42, use boost = 100
    //   - Rescored items will have scores like 104.17, 103.82, etc.
    //   - Easy to identify: score > 100 means rescored
    //   - Easy to extract raw rescore: 104.17 - 100 = 4.17
    //
    // We use item #100's (last rescored item) original BM25 score as the baseline.
    // Since BM25 ranking is monotonic, item #101+ will have BM25 ≤ item #100's BM25.
    // By ensuring rescored items > item #100's BM25, they'll rank above all non-rescored items.
    // Safety margin (1.0) ensures rescored items with small values (e.g., 0.5) still rank higher.
    final double SAFETY_MARGIN = 1.0;

    double maxNonRescoredScore = 0.0;
    // Use item #100's original BM25 score (before rescoring) as the baseline
    if (actualWindowSize > 0) {
      SearchEntity lastRescoredEntity = allEntities.get(actualWindowSize - 1);
      String lastDocId = lastRescoredEntity.getEntity().toString();

      // ES document IDs are URL-encoded, so we need to encode the URN to match
      try {
        lastDocId = java.net.URLEncoder.encode(lastDocId, "UTF-8");
      } catch (java.io.UnsupportedEncodingException e) {
        log.warn("Failed to URL-encode docId: {}", lastDocId, e);
      }

      SearchHit lastHit = hitMap.get(lastDocId);
      if (lastHit != null) {
        maxNonRescoredScore = lastHit.getScore(); // Original BM25 score of item #100
      }
    }

    // Find smallest power of 10 greater than (maxNonRescoredScore + SAFETY_MARGIN)
    // Examples: 45.2 + 1.0 = 46.2 → boost = 100
    //           4.5 + 1.0 = 5.5 → boost = 10
    //           450.2 + 1.0 = 451.2 → boost = 1000
    final double threshold = maxNonRescoredScore + SAFETY_MARGIN;
    final double rescoreBoost;
    if (threshold <= 1.0) {
      // For small/negative thresholds, use minimum boost of 1.0
      // This ensures rescored items always have a meaningful boost
      rescoreBoost = 1.0;
    } else {
      // Calculate ceiling power of 10: 10^ceil(log10(threshold))
      double power = Math.ceil(Math.log10(threshold));
      rescoreBoost = Math.pow(10, power);
    }

    // Rescore entities within window, keep rest with original scores
    List<Pair<SearchEntity, Double>> allScoredEntities = new ArrayList<>(totalEntities);
    int rescoredCount = 0;

    for (int i = 0; i < totalEntities; i++) {
      SearchEntity entity = allEntities.get(i);
      String docId = entity.getEntity().toString();

      // ES document IDs are URL-encoded, so we need to encode the URN to match
      try {
        docId = java.net.URLEncoder.encode(docId, "UTF-8");
      } catch (java.io.UnsupportedEncodingException e) {
        log.warn("Failed to URL-encode docId: {}", docId, e);
      }

      SearchHit hit = hitMap.get(docId);

      if (i < actualWindowSize && hit != null) {
        // Within window: apply rescoring + dynamic boost
        double esScore = hit.getScore();
        RescoreResult rescoreResult = rescorer.rescore(hit, esScore);
        double rawRescoreValue = rescoreResult.getFinalScore();

        // Add dynamic boost to ensure rescored items rank above non-rescored
        double boostedScore = rawRescoreValue + rescoreBoost;
        entity.setScore(boostedScore);

        // Add explanation if enabled - include boost info for debugging
        if (includeExplain) {
          try {
            // Create enhanced explanation with boost information
            Map<String, Object> explanationMap = new LinkedHashMap<>();
            explanationMap.put("documentId", rescoreResult.getDocumentId());
            explanationMap.put("bm25Score", rescoreResult.getBm25Score());
            explanationMap.put("rescoreValue", rawRescoreValue);
            explanationMap.put("rescoreBoost", rescoreBoost);
            explanationMap.put("finalScore", boostedScore);
            explanationMap.put("formula", rescoreResult.getFormula());
            explanationMap.put("signals", rescoreResult.getSignals());

            String explanation = RESCORE_MAPPER.writeValueAsString(explanationMap);
            StringMap extraFields = entity.getExtraFields();
            if (extraFields == null) {
              extraFields = new StringMap();
            }
            extraFields.put("_rescoreExplain", explanation);
            entity.setExtraFields(extraFields);
          } catch (JsonProcessingException e) {
            log.warn("Failed to serialize rescore explanation for {}", docId, e);
          }
        }

        allScoredEntities.add(Pair.of(entity, boostedScore));
        rescoredCount++;
      } else {
        // Outside window or no hit: keep original ES score (no boost)
        double originalScore = entity.getScore() != null ? entity.getScore() : 0.0;
        allScoredEntities.add(Pair.of(entity, originalScore));
      }
    }

    // Sort ALL entities by score (descending)
    // Rescored items (with boost) will naturally appear before non-rescored items
    allScoredEntities.sort(Comparator.comparing(Pair<SearchEntity, Double>::getRight).reversed());

    // Rebuild the entities array
    SearchEntityArray reorderedEntities = new SearchEntityArray();
    for (Pair<SearchEntity, Double> pair : allScoredEntities) {
      reorderedEntities.add(pair.getLeft());
    }
    result.setEntities(reorderedEntities);

    log.debug(
        "Applied Java rescoring to {} of {} entities (windowSize={}, actualWindow={}, boost={}) with formula: {}",
        rescoredCount,
        totalEntities,
        windowSize,
        actualWindowSize,
        String.format("%.3f", rescoreBoost),
        rescorer.getFormula().substring(0, Math.min(50, rescorer.getFormula().length())));

    return result;
  }

  /**
   * Calculate the number of results to fetch from ES for rescoring. We need to fetch enough results
   * to cover both: 1. The configured rescore window (top K to rescore) 2. The requested pagination
   * range (from + size)
   *
   * @param from Starting offset requested by user
   * @param size Number of results requested by user
   * @param configuredWindow Configured windowSize from rescore config
   * @param maxWindow Maximum allowed fetch size (cap for memory safety)
   * @return Number of results to fetch from ES
   */
  private int calculateRescoreWindow(
      int from, @Nullable Integer size, int configuredWindow, int maxWindow) {
    // Apply size limit
    int effectiveSize = ConfigUtils.applyLimit(searchServiceConfig, size);

    // Need to fetch at least up to the end of requested range
    int requestedEnd = from + effectiveSize;

    // Also need to fetch at least the configured window for rescoring
    int fetchSize = Math.max(configuredWindow, requestedEnd);

    // Cap at maxWindow for safety
    return Math.min(fetchSize, maxWindow);
  }

  /**
   * Slice a SearchResult to return only the requested page after rescoring.
   *
   * @param rescored Full SearchResult with all rescored entities
   * @param requestedFrom Starting offset requested by user
   * @param requestedSize Number of results requested by user
   * @return SearchResult containing only the requested page
   */
  private SearchResult sliceSearchResultForPagination(
      SearchResult rescored, int requestedFrom, @Nullable Integer requestedSize) {

    int effectiveSize = ConfigUtils.applyLimit(searchServiceConfig, requestedSize);
    List<SearchEntity> allEntities = rescored.getEntities();

    // Slice for requested page
    int startIndex = Math.min(requestedFrom, allEntities.size());
    int endIndex = Math.min(requestedFrom + effectiveSize, allEntities.size());
    List<SearchEntity> pageEntities = allEntities.subList(startIndex, endIndex);

    // Build result with paginated entities but preserve original metadata
    return rescored
        .setEntities(new SearchEntityArray(pageEntities))
        .setFrom(requestedFrom)
        .setPageSize(effectiveSize);
  }

  /**
   * Apply Java-based rescoring to scroll results.
   *
   * @param opContext operation context
   * @param searchResponse raw ES response containing SearchHits
   * @param result transformed scroll result
   * @return rescored scroll result with updated scores and explanations
   */
  private ScrollResult applyJavaRescore(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nonnull ScrollResult result) {
    // Convert ScrollResult to SearchResult, apply rescoring, then convert back
    SearchResult tempResult =
        new SearchResult()
            .setEntities(result.getEntities())
            .setMetadata(result.getMetadata())
            .setNumEntities(result.getNumEntities())
            .setPageSize(result.getPageSize());

    SearchResult rescored = applyJavaRescore(opContext, searchResponse, tempResult);

    result.setEntities(rescored.getEntities());
    return result;
  }

  /**
   * Resolves whether Search V2.5 is enabled for the current request. Priority: per-request
   * SearchFlags > server configuration
   *
   * @param opContext operation context containing search flags
   * @return true if V2.5 features should be used, false for V2 (main branch behavior)
   */
  private boolean resolveSearchV2_5Enabled(@Nonnull OperationContext opContext) {
    // Check per-request override first
    if (opContext.getSearchContext() != null
        && opContext.getSearchContext().getSearchFlags() != null
        && opContext.getSearchContext().getSearchFlags().hasSearchVersionV2_5()) {
      return opContext.getSearchContext().getSearchFlags().isSearchVersionV2_5();
    }

    // Fall back to server configuration (getSearch() can be null)
    return searchConfiguration.getSearch() != null
        && searchConfiguration.getSearch().isEnableSearchV2_5();
  }
}
