package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.X_CONTENT_REGISTRY;
import static com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder.URN_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.IncidentStats;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3DocumentIdResolver;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.QueryIntent;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.QueryUnderstanding;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
@RequiredArgsConstructor
@Accessors(chain = true)
public class ESSearchDAO {

  /**
   * Hybrid search reranks the first this many keyword rows, the search cache's default batch, and
   * keeps every later row in keyword order, so every page is a slice of the same ranking.
   */
  private static final int HYBRID_RERANK_WINDOW = 100;

  /** Time the embedding and kNN calls get before the keyword ranking is served instead. */
  private static final long HYBRID_TIMEOUT_MILLIS = 2_000;

  // Runs the embedding and kNN calls so a slow provider cannot hold a search past the timeout. The
  // calls end by the same deadline, so a worker is free about when its search falls back. The
  // queue is bounded: when every worker is busy, searches get the keyword ranking right away
  private static final ExecutorService HYBRID_EXECUTOR =
      new ThreadPoolExecutor(
          8,
          8,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(16),
          new ThreadFactoryBuilder().setNameFormat("hybrid-rerank-%d").setDaemon(true).build());

  /**
   * Queries containing 6+ consecutive digits are ID or hash lookups (e.g. "run_20240101_120000").
   * When the light query matches nothing for these, fuzzy expansion of the digit runs only adds
   * false positives, so the full query is skipped.
   */
  private static final Pattern HASH_ID_QUERY_PATTERN = Pattern.compile(".*\\d{6,}.*");

  private static final Pattern DELIMITER_PATTERN = Pattern.compile("[_\\-./:]+");

  /**
   * No-space queries of at least this many delimiter-separated tokens (long entity names, deep
   * FQNs) skip the full query when the light query matches nothing: the name does not exist, and
   * fuzzy expansion only finds noisy partial matches.
   */
  private static final int LONG_EXACT_NAME_TOKEN_THRESHOLD = 4;

  private final boolean pointInTimeCreationEnabled;
  @Nonnull private final ElasticSearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final boolean testLoggingEnabled;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;
  @Nonnull private final EntityDocumentIdHasher entityDocumentIdHasher;
  @Nullable private final HybridSearchResultReranker hybridSearchResultReranker;

  public ESSearchDAO(
      boolean pointInTimeCreationEnabled,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchServiceConfiguration searchServiceConfig) {
    this(
        pointInTimeCreationEnabled,
        searchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        false,
        searchServiceConfig,
        new Sha256UrnEntityDocumentIdHasher());
  }

  public ESSearchDAO(
      boolean pointInTimeCreationEnabled,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      boolean testLoggingEnabled,
      @Nonnull SearchServiceConfiguration searchServiceConfig) {
    this(
        pointInTimeCreationEnabled,
        searchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        testLoggingEnabled,
        searchServiceConfig,
        new Sha256UrnEntityDocumentIdHasher());
  }

  public ESSearchDAO(
      boolean pointInTimeCreationEnabled,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      boolean testLoggingEnabled,
      @Nonnull SearchServiceConfiguration searchServiceConfig,
      @Nonnull EntityDocumentIdHasher entityDocumentIdHasher) {
    this(
        pointInTimeCreationEnabled,
        searchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        testLoggingEnabled,
        searchServiceConfig,
        entityDocumentIdHasher,
        null);
  }

  @Nonnull
  private SearchClientShim<?> searchClient(
      @Nonnull OperationContext opContext, @Nonnull SearchRequest searchRequest) {
    return SearchClients.forEntityIndices(opContext, searchRequest, searchConfiguration);
  }

  @Nonnull
  private SearchClientShim<?> searchClient(
      @Nonnull OperationContext opContext, @Nullable String... indices) {
    return SearchClients.forEntityIndices(opContext, searchConfiguration, indices);
  }

  public long docCount(@Nonnull OperationContext opContext, @Nonnull String entityName) {
    return docCount(opContext, entityName, null);
  }

  public long docCount(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    CountRequest countRequest =
        new CountRequest(entityIndexName(opContext, entityName))
            .query(
                SearchRequestHandler.getFilterQuery(
                    opContext,
                    List.of(entityName),
                    filter,
                    entitySpec.getSearchableFieldTypes(),
                    queryFilterRewriteChain,
                    searchConfiguration.getEntityIndex()));

    return opContext.withSpan(
        "docCount",
        () -> {
          try {
            return searchClient(opContext, entityIndexName(opContext, entityName))
                .count(opContext, countRequest, RequestOptions.DEFAULT)
                .getCount();
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
      @Nullable Integer size,
      @Nullable QueryBuilder lightQuery,
      @Nonnull String input) {
    long id = System.currentTimeMillis();

    return opContext.withSpan(
        "executeAndExtract_search",
        () -> {
          SearchResponse searchResponse = null;
          try {
            log.debug("Executing request {}: {}", id, searchRequest);
            searchResponse =
                lightQuery == null
                    ? searchClient(opContext, searchRequest)
                        .search(opContext, searchRequest, RequestOptions.DEFAULT)
                    : searchLightFirst(opContext, searchRequest, lightQuery, input);
            // extract results, validated against document model as well
            return transformIndexIntoEntityName(
                opContext,
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
                        ConfigUtils.applyLimit(searchServiceConfig, size),
                        input));
          } catch (Exception e) {
            log.error("Search query failed", e);
            log.error("Response to the failed search query: {}", searchResponse);
            throw new ESQueryException("Search query failed:", e);
          } finally {
            log.debug("Returning from request {}.", id);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "executeAndExtract_search"));
  }

  private String transformIndexToken(
      @Nonnull OperationContext opContext,
      IndexConvention indexConvention,
      String name,
      int entityTypeIdx) {
    if (entityTypeIdx < 0) {
      return name;
    }
    String[] tokens = name.split(AGGREGATION_SEPARATOR_CHAR);
    if (entityTypeIdx < tokens.length) {
      tokens[entityTypeIdx] =
          indexConvention
              .getEntityName(opContext, tokens[entityTypeIdx])
              .orElse(tokens[entityTypeIdx]);
    }
    return String.join(AGGREGATION_SEPARATOR_CHAR, tokens);
  }

  private AggregationMetadata transformAggregationMetadata(
      @Nonnull OperationContext opContext,
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
                              transformIndexToken(
                                  opContext, indexConvention, entry.getKey(), entityTypeIdx),
                          Map.Entry::getValue))));
      aggMeta.setFilterValues(
          new FilterValueArray(
              aggMeta.getFilterValues().stream()
                  .map(
                      filterValue ->
                          filterValue.setValue(
                              transformIndexToken(
                                  opContext,
                                  indexConvention,
                                  filterValue.getValue(),
                                  entityTypeIdx)))
                  .collect(Collectors.toList())));
    }
    return aggMeta;
  }

  @VisibleForTesting
  public SearchResult transformIndexIntoEntityName(
      @Nonnull OperationContext opContext, IndexConvention indexConvention, SearchResult result) {
    return result.setMetadata(
        result
            .getMetadata()
            .setAggregations(
                transformIndexIntoEntityName(
                    opContext, indexConvention, result.getMetadata().getAggregations())));
  }

  private ScrollResult transformIndexIntoEntityName(
      @Nonnull OperationContext opContext, IndexConvention indexConvention, ScrollResult result) {
    return result.setMetadata(
        result
            .getMetadata()
            .setAggregations(
                transformIndexIntoEntityName(
                    opContext, indexConvention, result.getMetadata().getAggregations())));
  }

  private AggregationMetadataArray transformIndexIntoEntityName(
      @Nonnull OperationContext opContext,
      @Nonnull IndexConvention indexConvention,
      AggregationMetadataArray aggArray) {
    List<AggregationMetadata> newAggs = new ArrayList<>();
    for (AggregationMetadata aggMeta : aggArray) {
      List<String> aggregateFacets = List.of(aggMeta.getName().split(AGGREGATION_SEPARATOR_CHAR));
      int entityTypeIdx = aggregateFacets.indexOf(INDEX_VIRTUAL_FIELD);
      newAggs.add(transformAggregationMetadata(opContext, indexConvention, aggMeta, entityTypeIdx));
    }
    return new AggregationMetadataArray(newAggs);
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeAndExtract(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nullable String input) {
    return opContext.withSpan(
        "executeAndExtract_scroll",
        () -> {
          try {
            final SearchResponse searchResponse =
                searchClient(opContext, searchRequest)
                    .search(opContext, searchRequest, RequestOptions.DEFAULT);
            // extract results, validated against document model as well
            return transformIndexIntoEntityName(
                opContext,
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
                        pointInTimeCreationEnabled,
                        input));
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

    // A hybrid search fetches the keyword rows from the top to rerank them, then slices the page
    final int hybridFetchSize =
        hybridFetchSize(opContext, entityNames, input, sortCriteria, from, size);
    final int requestFrom = hybridFetchSize > 0 ? 0 : from;
    final Integer requestSize = hybridFetchSize > 0 ? hybridFetchSize : size;

    // Step 1: construct the query
    final Triple<SearchRequest, Filter, List<EntitySpec>> searchRequestComponents =
        opContext.withSpan(
            "searchRequest",
            () ->
                buildSearchRequest(
                    opContext,
                    entityNames,
                    input,
                    postFilters,
                    sortCriteria,
                    requestFrom,
                    requestSize,
                    facets),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(this.getClass(), "searchRequest"));

    if (testLoggingEnabled) {
      testLog(opContext.getObjectMapper(), searchRequestComponents.getLeft());
    }

    // Step 2: execute the query and extract results, validated against document model as well
    final SearchResult result =
        executeAndExtract(
            opContext,
            searchRequestComponents.getRight(),
            searchRequestComponents.getLeft(),
            searchRequestComponents.getMiddle(),
            requestFrom,
            requestSize,
            // A search without hits runs the full query, as in DataHub Cloud. The UI's facet counts
            // reach here through the search cache, which fetches hits, so they follow the light
            // query
            size != null && size == 0
                ? null
                : lightFirstQuery(
                    opContext,
                    searchRequestComponents.getRight(),
                    input,
                    sortCriteria,
                    postFilters),
            input);
    return hybridFetchSize > 0
        ? rerankHybrid(opContext, entityNames, input, result, from, size)
        : result;
  }

  /**
   * The number of keyword rows a hybrid search fetches, or 0 when the search stays keyword-only:
   * hybrid read is off, the page starts past the rerank window, no rows are requested, results are
   * not sorted by relevance, the input is not a full-text query, or no requested entity type has
   * vectors.
   */
  private int hybridFetchSize(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    if (hybridSearchResultReranker == null || from >= HYBRID_RERANK_WINDOW) {
      return 0;
    }
    final int pageSize = ConfigUtils.applyLimit(searchServiceConfig, size);
    final SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    final String trimmed = input.trim();
    if (pageSize == 0
        || !isRelevanceSort(sortCriteria)
        || searchFlags == null
        || !Boolean.TRUE.equals(searchFlags.isFulltext())
        || trimmed.isEmpty()
        || "*".equals(trimmed)
        || trimmed.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)) {
      return 0;
    }
    final int fetchSize = Math.max(HYBRID_RERANK_WINDOW, from + pageSize);
    // A fetch above the result limit would be cut short, or rejected in strict mode
    if (fetchSize > searchServiceConfig.getLimit().getResults().getMax()
        || hybridSearchResultReranker.vectorEntityNames(opContext, entityNames).isEmpty()) {
      return 0;
    }
    return fetchSize;
  }

  /**
   * Reranks the first {@link #HYBRID_RERANK_WINDOW} keyword rows with kNN scores and slices the
   * requested page. Totals and facets stay those of the keyword query. Any failure serves the
   * keyword ranking.
   */
  @Nonnull
  private SearchResult rerankHybrid(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nonnull SearchResult keywordResult,
      int from,
      @Nullable Integer size) {
    final List<SearchEntity> rows = keywordResult.getEntities();
    final int windowEnd = Math.min(HYBRID_RERANK_WINDOW, rows.size());
    List<SearchEntity> ranked = rows;
    Future<List<SearchEntity>> rerank = null;
    final Set<String> vectorEntityNames =
        hybridSearchResultReranker.vectorEntityNames(opContext, entityNames);
    final boolean windowHasVectorRows =
        rows.subList(0, windowEnd).stream()
            .anyMatch(
                row ->
                    row.getEntity() != null
                        && vectorEntityNames.contains(row.getEntity().getEntityType()));
    // A window without rows that have vectors makes no embedding or kNN call
    if (windowHasVectorRows) {
      final long deadlineNanos =
          System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HYBRID_TIMEOUT_MILLIS);
      try {
        // The worker gets its own copies: a rerank that finishes after the timeout must not change
        // the rows served as the keyword fallback
        final List<SearchEntity> window = new ArrayList<>(windowEnd);
        for (SearchEntity row : rows.subList(0, windowEnd)) {
          window.add(row.copy());
        }
        rerank =
            HYBRID_EXECUTOR.submit(
                Context.current()
                    .wrap(
                        () ->
                            hybridSearchResultReranker.rerank(
                                opContext,
                                entityNames,
                                input,
                                window,
                                List.of(URN_FIELD),
                                deadlineNanos)));
        ranked = new ArrayList<>(rerank.get(HYBRID_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        ranked.addAll(rows.subList(windowEnd, rows.size()));
        countHybrid(opContext, "hybridReadApplied");
      } catch (RejectedExecutionException e) {
        countHybrid(opContext, "hybridReadRejected");
      } catch (TimeoutException e) {
        // The interrupt frees a worker whose provider does not honor the deadline; a queued call
        // does not start
        rerank.cancel(true);
        countHybridTimeout(opContext);
      } catch (InterruptedException e) {
        rerank.cancel(true);
        Thread.currentThread().interrupt();
        countHybrid(opContext, "hybridReadFailed");
      } catch (Exception e) {
        final Throwable cause =
            e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (System.nanoTime() - deadlineNanos >= 0) {
          // The embedding or kNN call gave up at the deadline, just as the search did
          countHybridTimeout(opContext);
        } else {
          countHybrid(opContext, "hybridReadFailed");
          // One line per failed search; the stack trace only at debug, so an outage does not
          // flood logs
          log.warn("Hybrid read failed; serving the keyword ranking: {}", cause.toString());
          log.debug("Hybrid read failure", cause);
        }
      }
    }
    final int pageSize = ConfigUtils.applyLimit(searchServiceConfig, size);
    final int pageStart = Math.min(from, ranked.size());
    final int pageEnd = (int) Math.min((long) from + pageSize, ranked.size());
    return keywordResult
        .setEntities(new SearchEntityArray(ranked.subList(pageStart, pageEnd)))
        .setFrom(from)
        .setPageSize(pageSize);
  }

  private static void countHybridTimeout(@Nonnull OperationContext opContext) {
    countHybrid(opContext, "hybridReadTimeout");
    log.warn("Hybrid read took over {} ms; serving the keyword ranking.", HYBRID_TIMEOUT_MILLIS);
  }

  private static void countHybrid(@Nonnull OperationContext opContext, @Nonnull String metric) {
    opContext
        .getMetricUtils()
        .ifPresent(metricUtils -> metricUtils.increment(ESSearchDAO.class, metric, 1));
  }

  /**
   * The light Stage 1 query that Search V3 keyword reads run before the full query, or null when
   * the full query runs directly: on V2, with a sort order other than relevance, under a Column
   * Name filter, for searches that are not full-text, for structured queries, and for empty,
   * match-all, quoted and URN or path queries, whose light and full queries are the same or whose
   * quotes ask for exact matches only.
   */
  @VisibleForTesting
  @Nullable
  QueryBuilder lightFirstQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String input,
      @Nullable List<SortCriterion> sortCriteria,
      @Nullable Filter filter) {
    String trimmed = input.trim();
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    if (!EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex())
        || !isRelevanceSort(sortCriteria)
        // The name-focused light query would hide every dataset that only holds the column
        || hasColumnNameFilter(filter)
        || searchFlags == null
        || !Boolean.TRUE.equals(searchFlags.isFulltext())
        || trimmed.isEmpty()
        || "*".equals(trimmed)
        || trimmed.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)
        || isQuotedPhrase(trimmed)
        || QueryUnderstanding.understand(trimmed) == QueryIntent.IDENTITY) {
      return null;
    }
    return SearchRequestHandler.getBuilder(
            opContext,
            entitySpecs,
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            searchServiceConfig)
        .getQuery(opContext, input, true, true);
  }

  /**
   * Light-first relaxation: runs the light Stage 1 query (no fuzzy, wildcard or synonym-priority
   * clauses) and runs the full query only when the light query matches nothing. The response comes
   * from a single query, so its hits, total and facets all describe the query that was served.
   */
  @VisibleForTesting
  SearchResponse searchLightFirst(
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequest searchRequest,
      @Nonnull QueryBuilder lightQuery,
      @Nonnull String input)
      throws IOException {
    QueryBuilder fullQuery = searchRequest.source().query();
    QueryBuilder lightSourceQuery = buildLightSourceQuery(fullQuery, lightQuery);
    if (lightSourceQuery == null) {
      countLightFirst(opContext, "direct");
      return searchClient(opContext, searchRequest)
          .search(opContext, searchRequest, RequestOptions.DEFAULT);
    }
    SearchResponse lightResponse;
    searchRequest.source().query(lightSourceQuery);
    try {
      lightResponse =
          searchClient(opContext, searchRequest)
              .search(opContext, searchRequest, RequestOptions.DEFAULT);
    } finally {
      searchRequest.source().query(fullQuery);
    }
    if (lightResponse.getFailedShards() > 0) {
      countLightFirst(opContext, "shardFailure");
      log.warn(
          "Light query failed on {} of {} shards: {}",
          lightResponse.getFailedShards(),
          lightResponse.getTotalShards(),
          lightResponse.getShardFailures().length > 0
              ? lightResponse.getShardFailures()[0].reason()
              : "");
    }
    if (hasHits(lightResponse)) {
      countLightFirst(opContext, "light");
      return lightResponse;
    }
    if (stopsOnEmpty(lightResponse, input)) {
      countLightFirst(opContext, "stopped");
      return lightResponse;
    }
    countLightFirst(opContext, "full");
    log.debug("Light query matched nothing, running the full query for \"{}\"", input);
    return searchClient(opContext, searchRequest)
        .search(opContext, searchRequest, RequestOptions.DEFAULT);
  }

  /**
   * Whether the light query's empty result stands: fuzzy expansion only adds noise for ID or hash
   * lookups and for long names that the light query did not find.
   */
  @VisibleForTesting
  static boolean skipsFullQuery(@Nonnull String input) {
    String trimmed = input.trim();
    if (trimmed.chars().anyMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c))) {
      return false;
    }
    return HASH_ID_QUERY_PATTERN.matcher(trimmed).matches()
        || Arrays.stream(DELIMITER_PATTERN.split(trimmed)).filter(part -> !part.isEmpty()).count()
            >= LONG_EXACT_NAME_TOKEN_THRESHOLD;
  }

  /** Whether an empty light result stands: an ID or long-name input, and every shard in time. */
  private static boolean stopsOnEmpty(@Nonnull SearchResponse response, @Nonnull String input) {
    return skipsFullQuery(input) && response.getFailedShards() == 0 && !response.isTimedOut();
  }

  /** Total hits decide, not the page's hits: a later page of a light result can be empty. */
  private static boolean hasHits(@Nonnull SearchResponse response) {
    TotalHits totalHits = response.getHits().getTotalHits();
    return totalHits != null
        ? totalHits.value > 0
        : response.getHits().getHits() != null && response.getHits().getHits().length > 0;
  }

  /**
   * The search request's query with the light query in place of the full one: the filters of the
   * bool root are kept, and so is a function_score wrapper around it, the request shape of DataHub
   * Cloud (OSS requests have a bool root).
   */
  @VisibleForTesting
  @Nullable
  static QueryBuilder buildLightSourceQuery(
      @Nonnull final QueryBuilder originalQuery, @Nonnull final QueryBuilder lightQuery) {
    FunctionScoreQueryBuilder originalFunctionScoreQuery = null;
    QueryBuilder boolCarrier = originalQuery;
    if (originalQuery instanceof FunctionScoreQueryBuilder) {
      originalFunctionScoreQuery = (FunctionScoreQueryBuilder) originalQuery;
      boolCarrier = originalFunctionScoreQuery.query();
    }

    // Only the one must clause is replaced and the filters copied. Any other shape fails closed:
    // the caller runs the full query
    if (!(boolCarrier instanceof BoolQueryBuilder)
        || ((BoolQueryBuilder) boolCarrier).must().size() != 1
        || !((BoolQueryBuilder) boolCarrier).should().isEmpty()) {
      return null;
    }

    BoolQueryBuilder lightBool = QueryBuilders.boolQuery().must(lightQuery);
    for (QueryBuilder filter : ((BoolQueryBuilder) boolCarrier).filter()) {
      lightBool.filter(filter);
    }
    for (QueryBuilder mustNot : ((BoolQueryBuilder) boolCarrier).mustNot()) {
      lightBool.mustNot(mustNot);
    }

    if (originalFunctionScoreQuery == null) {
      return lightBool;
    }

    FunctionScoreQueryBuilder lightFunctionScoreQuery =
        QueryBuilders.functionScoreQuery(
            lightBool, originalFunctionScoreQuery.filterFunctionBuilders());
    lightFunctionScoreQuery
        .scoreMode(originalFunctionScoreQuery.scoreMode())
        .boostMode(originalFunctionScoreQuery.boostMode())
        .maxBoost(originalFunctionScoreQuery.maxBoost())
        .boost(originalFunctionScoreQuery.boost());
    if (originalFunctionScoreQuery.getMinScore() != null) {
      lightFunctionScoreQuery.setMinScore(originalFunctionScoreQuery.getMinScore());
    }
    if (originalFunctionScoreQuery.queryName() != null) {
      lightFunctionScoreQuery.queryName(originalFunctionScoreQuery.queryName());
    }
    return lightFunctionScoreQuery;
  }

  /**
   * Counts which query served a search that built a light query: light, full (fell through),
   * stopped, or direct (a request shape the light query cannot replace), and light queries with
   * failed shards. A search the light query does not apply to, including one whose custom
   * configuration leaves the light query no clause, runs the full query uncounted.
   */
  private static void countLightFirst(@Nonnull OperationContext opContext, @Nonnull String served) {
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils -> metricUtils.increment(ESSearchDAO.class, "lightFirst_" + served, 1));
  }

  /** No sort, or only by descending score (the explain API's default), orders by relevance. */
  private static boolean isRelevanceSort(@Nullable List<SortCriterion> sortCriteria) {
    return sortCriteria == null
        || sortCriteria.stream()
            .allMatch(
                criterion ->
                    "_score".equals(criterion.getField())
                        && criterion.getOrder()
                            != com.linkedin.metadata.query.filter.SortOrder.ASCENDING);
  }

  /**
   * Whether the filter requires a column name: a positive criterion on fieldPaths, which is what
   * the UI's Column Name filter sends. Covers both the or-of-and form and legacy criteria.
   */
  @VisibleForTesting
  static boolean hasColumnNameFilter(@Nullable final Filter filter) {
    if (filter == null) {
      return false;
    }
    final Stream<Criterion> criteria =
        Stream.concat(
            filter.hasOr()
                ? filter.getOr().stream().flatMap(conjunction -> conjunction.getAnd().stream())
                : Stream.empty(),
            filter.hasCriteria() ? filter.getCriteria().stream() : Stream.empty());
    return criteria.anyMatch(
        criterion ->
            !Boolean.TRUE.equals(criterion.isNegated())
                && ("fieldPaths".equals(criterion.getField())
                    || criterion.getField().startsWith("fieldPaths.")));
  }

  /** Returns true if the query is wrapped in double or single quotes. */
  private static boolean isQuotedPhrase(@Nonnull final String trimmedQuery) {
    return (trimmedQuery.startsWith("\"") && trimmedQuery.endsWith("\""))
        || (trimmedQuery.startsWith("'") && trimmedQuery.endsWith("'"));
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
    Filter transformedFilters = transformFilter(opContext, postFilters, indexConvention);

    SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getSearchRequest(
                opContext, finalInput, transformedFilters, sortCriteria, from, size, facets)
            .indices(entityIndexNames(opContext, entityNames));

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
    Filter transformedFilters = transformFilter(opContext, filters, indexConvention);
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getFilterRequest(opContext, transformedFilters, sortCriteria, from, size);

    searchRequest.indices(entityIndexName(opContext, entityName));
    return executeAndExtract(
        opContext, List.of(entitySpec), searchRequest, transformedFilters, from, size, null, "");
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
          searchClient(opContext, searchRequestAndBuilder.getLeft())
              .search(opContext, searchRequestAndBuilder.getLeft(), RequestOptions.DEFAULT);
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
            transformFilter(opContext, requestParams, indexConvention),
            limit);
    req.indices(entityIndexName(opContext, entityName));
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
                searchClient(opContext, searchRequest)
                    .search(opContext, searchRequest, RequestOptions.DEFAULT);
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
                transformFilter(opContext, requestParams, indexConvention),
                limit);
    // An empty list must fall through to the operation-scoped patterns, not to the else branch:
    // an empty indices array makes Elasticsearch search ALL indices, letting aggregates span
    // prefixes. Mirror the null handling of the entitySpec branch above.
    if (entityNames == null || entityNames.isEmpty()) {
      searchRequest.indices(
          EntitySearchIndexResolver.allEntityIndexPattern(
              opContext, searchConfiguration.getEntityIndex()));
    } else {
      searchRequest.indices(entityIndexNames(opContext, entityNames));
    }
    return searchRequest;
  }

  static final String INCIDENT_ENTITIES_FIELD = "entities.keyword";
  static final String INCIDENT_STATE_FIELD = "state";
  static final String INCIDENT_LAST_UPDATED_FIELD = "lastUpdated";
  static final String INCIDENT_ACTIVE_STATE = "ACTIVE";
  static final String BY_ENTITY_AGG = "byEntity";
  static final String LATEST_INCIDENT_AGG = "latestIncident";

  /**
   * Max entity URNs per active-incident-stats request. The batch size on the health {@code
   * DataLoader} that calls this is unbounded, so a large search page would otherwise send its whole
   * URN set in one request. Both ES limits this query is exposed to default to 65536: {@code
   * index.max_terms_count} (the {@code entities.keyword} terms filter) and {@code
   * search.max_buckets} (the by-entity aggregation materialises one bucket, with a {@code top_hits}
   * sub-agg, per URN). Partitioning keeps each request comfortably under both, mirroring {@code
   * LineageSearchService.MAX_TERMS} and the assertion-run batch path.
   */
  @VisibleForTesting static final int INCIDENT_STATS_URN_BATCH_SIZE = 1000;

  @WithSpan
  @Nonnull
  public Map<Urn, IncidentStats> getActiveIncidentStats(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> entityUrns) {
    if (entityUrns.isEmpty()) {
      return Map.of();
    }
    return opContext.withSpan(
        "getActiveIncidentStats_search",
        () -> {
          try {
            final Map<Urn, IncidentStats> result = new HashMap<>();
            for (List<Urn> batch :
                Lists.partition(new ArrayList<>(entityUrns), INCIDENT_STATS_URN_BATCH_SIZE)) {
              final SearchRequest searchRequest =
                  buildActiveIncidentStatsRequest(opContext, new HashSet<>(batch));
              final SearchResponse searchResponse =
                  searchClient(opContext, searchRequest)
                      .search(opContext, searchRequest, RequestOptions.DEFAULT);
              result.putAll(extractIncidentStats(searchResponse));
            }
            return result;
          } catch (Exception e) {
            log.error("Active incident stats query failed", e);
            throw new ESQueryException("Active incident stats query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getActiveIncidentStats_search"));
  }

  @VisibleForTesting
  public SearchRequest buildActiveIncidentStatsRequest(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> entityUrns) {
    final String[] urnStrings = entityUrns.stream().map(Urn::toString).toArray(String[]::new);

    final BoolQueryBuilder query =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(INCIDENT_STATE_FIELD, INCIDENT_ACTIVE_STATE))
            .filter(QueryBuilders.termsQuery(INCIDENT_ENTITIES_FIELD, urnStrings));

    // This aggregation bypasses SearchRequestHandler, so apply the same soft-delete / hidden-stage
    // defaults the unbatched entityClient.filter path gets, keeping the two paths in agreement.
    // A no-op while the incident entity declares no status aspect (hence no indexed `removed`
    // field), but it means the batched counts cannot drift from the per-entity query if it does.
    ESUtils.applyDefaultSearchFilters(
        opContext,
        List.of(Constants.INCIDENT_ENTITY_NAME),
        null,
        query,
        searchConfiguration.getEntityIndex());

    final TermsAggregationBuilder byEntity =
        AggregationBuilders.terms(BY_ENTITY_AGG)
            .field(INCIDENT_ENTITIES_FIELD)
            .includeExclude(new IncludeExclude(urnStrings, null))
            .size(entityUrns.size())
            .subAggregation(
                AggregationBuilders.topHits(LATEST_INCIDENT_AGG)
                    .size(1)
                    .sort(SortBuilders.fieldSort(INCIDENT_LAST_UPDATED_FIELD).order(SortOrder.DESC))
                    .fetchSource("urn", null));

    final SearchSourceBuilder source =
        new SearchSourceBuilder().size(0).query(query).aggregation(byEntity);

    final IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    final SearchRequest request =
        new SearchRequest(entityIndexName(opContext, Constants.INCIDENT_ENTITY_NAME));
    request.source(source);
    return request;
  }

  private static Map<Urn, IncidentStats> extractIncidentStats(
      @Nonnull SearchResponse searchResponse) {
    final Map<Urn, IncidentStats> result = new HashMap<>();
    if (searchResponse.getAggregations() == null) {
      return result;
    }
    final Terms byEntity = searchResponse.getAggregations().get(BY_ENTITY_AGG);
    if (byEntity == null) {
      return result;
    }
    for (Terms.Bucket bucket : byEntity.getBuckets()) {
      final Urn entityUrn = UrnUtils.getUrn(bucket.getKeyAsString());
      Urn latestIncidentUrn = null;
      final TopHits topHits = bucket.getAggregations().get(LATEST_INCIDENT_AGG);
      if (topHits != null && topHits.getHits().getHits().length > 0) {
        final Object urnValue = topHits.getHits().getHits()[0].getSourceAsMap().get("urn");
        if (urnValue != null) {
          latestIncidentUrn = UrnUtils.getUrn(urnValue.toString());
        }
      }
      result.put(entityUrn, new IncidentStats((int) bucket.getDocCount(), latestIncidentUrn));
    }
    return result;
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
        size,
        input);
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

    String[] indexArray = entityIndexNames(opContext, entities);

    Filter transformedFilters = transformFilter(opContext, postFilters, indexConvention);

    boolean hasSliceOptions = opContext.getSearchContext().getSearchFlags().hasSliceOptions();

    boolean usePIT = (pointInTimeCreationEnabled || hasSliceOptions) && keepAlive != null;
    String pitId =
        usePIT
            ? ESUtils.computePointInTime(
                opContext, scrollId, keepAlive, searchClient(opContext, indexArray), indexArray)
            : null;
    Object[] sort = scrollId != null ? SearchAfterWrapper.fromScrollId(scrollId).getSort() : null;

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
                finalInput,
                transformedFilters,
                sortCriteria,
                sort,
                pitId,
                keepAlive,
                size,
                facets);

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

                String resolvedIndex =
                    opContext
                        .getSearchContext()
                        .getIndexConvention()
                        .getIndexName(opContext, indexName);
                SearchRequest searchRequest = new SearchRequest(resolvedIndex);
                searchRequest.source(searchSourceBuilder);

                return SearchClients.forIndex(opContext, searchConfiguration, resolvedIndex)
                    .search(opContext, searchRequest, RequestOptions.DEFAULT);
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
                String indexName = entityIndexName(opContext, entry.getValue().getName());

                BoolQueryBuilder query =
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(URN_FIELD, entry.getKey().toString()));
                EntitySearchIndexResolver.applyEntityTypeFilter(
                    query,
                    List.of(entry.getValue().getName()),
                    searchConfiguration.getEntityIndex());

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(query);

                SearchRequest searchRequest = new SearchRequest(indexName);
                searchRequest.source(searchSourceBuilder);

                return Map.entry(
                    entry.getKey(),
                    searchClient(opContext, searchRequest)
                        .search(opContext, searchRequest, RequestOptions.DEFAULT));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
        .id(documentIdForExplain(opContext, documentId))
        .index(entityIndexName(opContext, entityName));
    try {
      explainRequest.query(
          servedQuery(
              opContext,
              explainRequest.index(),
              searchRequest.getLeft().source().query(),
              // Scroll (a scroll id or a keep-alive) and searches without hits never run the light
              // query
              scrollId != null || keepAlive != null || (size != null && size == 0)
                  ? null
                  : lightFirstQuery(
                      opContext, searchRequest.getRight(), query, sortCriteria, postFilters),
              query));
      return searchClient(opContext, explainRequest.index())
          .explain(opContext, explainRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to explain query.", e);
      throw new IllegalStateException("Failed to explain query:", e);
    }
  }

  /**
   * The query a search for the same input would serve: the light query when it matches anything
   * (counted without fetching hits), else the full query, as {@link #searchLightFirst} decides.
   */
  private QueryBuilder servedQuery(
      @Nonnull OperationContext opContext,
      @Nonnull String index,
      @Nonnull QueryBuilder fullQuery,
      @Nullable QueryBuilder lightQuery,
      @Nonnull String input)
      throws IOException {
    if (lightQuery == null) {
      return fullQuery;
    }
    QueryBuilder lightSourceQuery = buildLightSourceQuery(fullQuery, lightQuery);
    if (lightSourceQuery == null) {
      return fullQuery;
    }
    SearchRequest countRequest =
        new SearchRequest(index)
            .source(
                new SearchSourceBuilder().query(lightSourceQuery).size(0).trackTotalHitsUpTo(1));
    SearchResponse count =
        searchClient(opContext, countRequest)
            .search(opContext, countRequest, RequestOptions.DEFAULT);
    // The decision searchLightFirst makes
    return hasHits(count) || stopsOnEmpty(count, input) ? lightSourceQuery : fullQuery;
  }

  /**
   * V3 stores documents under a hashed {@code _id}. Explain callers historically passed a URN or V2
   * URL-encoded URN; convert those when V3 keyword reads are on. An already-hashed id is left
   * unchanged.
   */
  @VisibleForTesting
  String documentIdForExplain(@Nonnull OperationContext opContext, @Nonnull String documentId) {
    if (!EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex())) {
      return documentId;
    }
    return V3DocumentIdResolver.resolveExplainDocumentId(
        opContext, entityDocumentIdHasher, documentId);
  }

  private boolean rewriteEntityTypeToIndex() {
    return !EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex());
  }

  /**
   * V2 rewrites {@code _entityType} filters onto index names. V3 documents store the entity type,
   * so the filter is normalized for the V3 fields instead. Doing it here gives the query and the
   * facets extracted from the response the same filter values. The request handlers normalize
   * again, so the normalization must stay idempotent.
   */
  @Nullable
  private Filter transformFilter(
      @Nonnull OperationContext opContext,
      @Nullable Filter filter,
      @Nonnull IndexConvention indexConvention) {
    return rewriteEntityTypeToIndex()
        ? transformFilterForEntities(opContext, filter, indexConvention)
        : ESUtils.toV3EntityFilter(opContext, filter);
  }

  @Nonnull
  private String[] entityIndexNames(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> entityNames) {
    return EntitySearchIndexResolver.indexNames(
        opContext, entityNames, searchConfiguration.getEntityIndex());
  }

  @Nonnull
  private String entityIndexName(@Nonnull OperationContext opContext, @Nonnull String entityName) {
    return EntitySearchIndexResolver.indexName(
        opContext, entityName, searchConfiguration.getEntityIndex());
  }

  private void testLog(ObjectMapper mapper, SearchRequest searchRequest) {
    try {
      log.warn("SearchRequest(custom): {}", mapper.writeValueAsString(customSearchConfiguration));
      final String[] indices = searchRequest.indices();
      log.warn(
          String.format(
              "SearchRequest(indices): %s",
              mapper.writerWithDefaultPrettyPrinter().writeValueAsString(indices)));
      log.warn(
          String.format(
              "SearchRequest(query): %s",
              mapper.writeValueAsString(mapper.readTree(searchRequest.source().toString()))));
    } catch (JsonProcessingException e) {
      log.warn("Error writing test log");
    }
  }
}
