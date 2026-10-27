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
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3DocumentIdResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.PreparedSearchRequest;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.EntityFilter;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.HighlightCarry;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchClauseBudget;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryIntent;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryUnderstanding;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateBootstrap;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponent;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentPage;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRequest;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponents;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateContextVariables;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLight;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMustache;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateQueryShape;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRollout;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRuntimes;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateWalk;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.search.utils.SearchOperation;
import com.linkedin.metadata.search.utils.SearchTookMetrics;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.search.template.SearchTemplateComponentMode;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
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
   * keeps every later row in keyword order, so every page is a slice of the same ranking. A page
   * served the keyword ranking instead (its rerank failed, or hybrid read is paused) is a slice of
   * that ranking, so across pages served differently a document can show twice or not at all.
   */
  private static final int HYBRID_RERANK_WINDOW = 100;

  /** Time the embedding and kNN calls get before the keyword ranking is served instead. */
  private static final long HYBRID_TIMEOUT_MILLIS = 2_000;

  /**
   * Hybrid failures in a row (timeouts and errors) after which hybrid read pauses. Failures in a
   * row catch an outage; a provider that only sometimes fails rarely fails three times in a row, so
   * its searches keep the timeout as their bound rather than pausing hybrid read for everyone.
   */
  private static final int HYBRID_FAILURES_BEFORE_PAUSE = 3;

  /**
   * How long hybrid read pauses after repeated failures, in this GMS process: while a slow provider
   * recovers, searches neither call it nor hold their request thread for the timeout. When the
   * pause ends, searches try hybrid read again until three fail in a row, so in a lasting outage
   * searches still wait out the timeout between pauses.
   */
  private static final long HYBRID_PAUSE_MILLIS = 30_000;

  // Runs the embedding and kNN calls so a slow provider cannot hold a search past the timeout. The
  // remote calls end by the same deadline, so a worker is free about when its search falls back;
  // the in-process providers ignore it and keep their worker until they finish. The queue is
  // bounded: when the workers and the queue are full, searches get the keyword ranking right away
  private static final ExecutorService HYBRID_EXECUTOR =
      new ThreadPoolExecutor(
          8,
          8,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(16),
          new ThreadFactoryBuilder().setNameFormat("hybrid-rerank-%d").setDaemon(true).build());

  private final boolean pointInTimeCreationEnabled;
  @Nonnull private final ElasticSearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final boolean testLoggingEnabled;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;
  @Nonnull private final EntityDocumentIdHasher entityDocumentIdHasher;
  @Nullable private final HybridSearchResultReranker hybridSearchResultReranker;

  private final AtomicInteger hybridFailures = new AtomicInteger();
  private final AtomicLong hybridPausedUntilNanos = new AtomicLong(System.nanoTime());
  private final AtomicLong hybridLastFailureNanos = new AtomicLong(System.nanoTime());
  private volatile long hybridPauseNanos = TimeUnit.MILLISECONDS.toNanos(HYBRID_PAUSE_MILLIS);
  private volatile long hybridTimeoutMillis = HYBRID_TIMEOUT_MILLIS;

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

  /** Script field under which {@link #aspectFieldNamesByDocumentId} reads a document's aspects. */
  private static final String ASPECT_FIELD_NAMES = "aspectFieldNames";

  /**
   * Map key meaning the document still has {@code _aspects.structuredProperties}. Not an aspect
   * name, and not the list of root structured-property keys.
   */
  public static final String STALE_ASPECTS_STRUCTURED_PROPERTIES = "_aspects.structuredProperties";

  /**
   * Separates an object field from a key inside it in the names this script reports ({@code
   * customProperties<separator>oldKey}). A dot cannot do that: searchable field names contain dots,
   * and a dotted {@code structuredProperties.*} key is a root field name, not a child of an object.
   */
  public static final String NESTED_FIELD_SEPARATOR = "\u001f";

  /**
   * Aspect names under {@code _aspects}, each with the field names under it. An object field also
   * contributes each key inside it, joined with {@link #NESTED_FIELD_SEPARATOR}, so a catch-up can
   * null a map entry the restate dropped. Structured properties live at the root as dotted keys
   * ({@code structuredProperties.<name>}), so that entry is those keys: every top-level {@code
   * _source} key with that prefix, plus each child when {@code _source.structuredProperties} is
   * itself a map. A leftover {@code _aspects.structuredProperties} is reported under {@link
   * #STALE_ASPECTS_STRUCTURED_PROPERTIES} and does not replace that list. Names only.
   */
  private static final String ASPECT_FIELD_NAMES_SCRIPT =
      "Map names = new HashMap();"
          + " def source = params['_source'];"
          + " if (!(source instanceof Map)) { return names; }"
          + " def sep = '"
          + NESTED_FIELD_SEPARATOR
          + "';"
          + " def aspects = source['_aspects'];"
          + " if (aspects instanceof Map) {"
          + "   for (def entry : aspects.entrySet()) {"
          + "     if ('structuredProperties'.equals(entry.getKey())) {"
          + "       names['"
          + STALE_ASPECTS_STRUCTURED_PROPERTIES
          + "'] = new ArrayList();"
          + "       continue;"
          + "     }"
          + "     def pending = new ArrayList();"
          + "     def prefixes = new ArrayList();"
          + "     pending.add(entry.getValue());"
          + "     prefixes.add('');"
          + "     def collected = new ArrayList();"
          + "     while (!pending.isEmpty()) {"
          + "       def current = pending.remove(pending.size() - 1);"
          + "       def prefix = prefixes.remove(prefixes.size() - 1);"
          + "       if (!(current instanceof Map)) { continue; }"
          + "       for (def child : current.entrySet()) {"
          + "         if (!(child.getKey() instanceof String)) { continue; }"
          + "         def key = (String) child.getKey();"
          + "         def path = prefix == '' ? key : prefix + sep + key;"
          + "         collected.add(path);"
          + "         if (child.getValue() instanceof Map) {"
          + "           pending.add(child.getValue());"
          + "           prefixes.add(path);"
          + "         }"
          + "       }"
          + "     }"
          + "     names[entry.getKey()] = collected;"
          + "   }"
          + " }"
          + " def dotted = new ArrayList();"
          + " for (def key : source.keySet()) {"
          + "   if (key instanceof String && ((String) key).startsWith('structuredProperties.')) {"
          + "     dotted.add(key);"
          + "   }"
          + " }"
          + " def nested = source['structuredProperties'];"
          + " if (nested instanceof Map) {"
          + "   for (def child : nested.keySet()) {"
          + "     def dottedKey = 'structuredProperties.' + child;"
          + "     if (!dotted.contains(dottedKey)) { dotted.add(dottedKey); }"
          + "   }"
          + " }"
          + " if (!dotted.isEmpty()) { names['structuredProperties'] = dotted; }"
          + " return names;";

  /** The painless source {@link #aspectFieldNamesByDocumentId} runs. Visible for its contract. */
  static String aspectFieldNamesScript() {
    return ASPECT_FIELD_NAMES_SCRIPT;
  }

  @Nonnull
  private SearchClientShim<?> searchClient(
      @Nonnull OperationContext opContext, @Nullable String... indices) {
    return SearchClients.forEntityIndices(opContext, searchConfiguration, indices);
  }

  /**
   * For each of {@code documentIds} present in {@code indexName}, the aspects its document holds,
   * as aspect name to the field names the document holds for it: the entries under {@code
   * _aspects}, each field inside an object field joined to its parent with {@link
   * #NESTED_FIELD_SEPARATOR}, the structured properties aspect as the dotted root keys it owns, and
   * a leftover {@code _aspects.structuredProperties} entry under {@link
   * #STALE_ASPECTS_STRUCTURED_PROPERTIES}. One request for the whole set, and names only: the
   * engine reads each document's source map in a script field and sends no document body back. A
   * document that is not in the index has no entry.
   */
  @Nonnull
  public Map<String, Map<String, Set<String>>> aspectFieldNamesByDocumentId(
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull Collection<String> documentIds) {
    if (documentIds.isEmpty()) {
      return Map.of();
    }
    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(QueryBuilders.idsQuery().addIds(documentIds.toArray(new String[0])))
            .size(documentIds.size())
            .fetchSource(false)
            .scriptField(
                ASPECT_FIELD_NAMES,
                new Script(ScriptType.INLINE, "painless", ASPECT_FIELD_NAMES_SCRIPT, Map.of()));
    SearchRequest request = new SearchRequest(indexName).source(source);
    SearchResponse response;
    try {
      response =
          searchClient(opContext, indexName).search(opContext, request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new ESQueryException("Failed to read aspect names from " + indexName, e);
    }
    Map<String, Map<String, Set<String>>> byDocumentId = new HashMap<>();
    for (SearchHit hit : response.getHits().getHits()) {
      byDocumentId.put(hit.getId(), aspectFieldNames(hit.getFields().get(ASPECT_FIELD_NAMES)));
    }
    return byDocumentId;
  }

  @Nonnull
  @VisibleForTesting
  static Map<String, Set<String>> aspectFieldNames(@Nullable DocumentField field) {
    Map<String, Set<String>> byAspect = new HashMap<>();
    Object value = field == null ? null : field.getValue();
    if (!(value instanceof Map<?, ?> aspects)) {
      return byAspect;
    }
    aspects.forEach(
        (aspect, fields) ->
            byAspect.put(
                String.valueOf(aspect),
                fields instanceof Collection<?> names
                    ? names.stream().map(String::valueOf).collect(Collectors.toSet())
                    : Set.of()));
    return byAspect;
  }

  public long docCount(@Nonnull OperationContext opContext, @Nonnull String entityName) {
    return docCount(opContext, entityName, null);
  }

  public long docCount(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    CountRequest countRequest = buildDocCountRequest(opContext, entityName, filter);

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
                    ? SearchTookMetrics.time(
                        opContext,
                        SearchOperation.ENTITY_SEARCH,
                        () ->
                            searchClient(opContext, searchRequest)
                                .search(opContext, searchRequest, RequestOptions.DEFAULT))
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
                SearchTookMetrics.time(
                    opContext,
                    SearchOperation.ENTITY_SCROLL,
                    () ->
                        searchClient(opContext, searchRequest)
                            .search(opContext, searchRequest, RequestOptions.DEFAULT));
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
    HighlightCarry.begin();
    try {
      return applyHighlightCarry(
          opContext,
          searchRanked(
              opContext, entityNames, input, postFilters, sortCriteria, from, size, facets));
    } finally {
      HighlightCarry.clear();
    }
  }

  @Nonnull
  private SearchResult searchRanked(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    if (templateReads(opContext)) {
      final int hybridFetchSize =
          hybridFetchSize(opContext, entityNames, input, sortCriteria, from, size);
      final int requestFrom = hybridFetchSize > 0 ? 0 : from;
      final Integer requestSize = hybridFetchSize > 0 ? hybridFetchSize : size;
      final BoundSearch searched =
          searchWithTemplates(
              opContext,
              entityNames,
              input,
              postFilters,
              sortCriteria,
              requestFrom,
              requestSize,
              facets);
      return finishTemplateHybrid(
          searched.context(), entityNames, input, searched.result(), from, size, hybridFetchSize);
    }

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

  @Nonnull
  private SearchResult applyHighlightCarry(
      @Nonnull OperationContext opContext, @Nonnull SearchResult result) {
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    boolean explain = flags != null && Boolean.TRUE.equals(flags.isIncludeExplain());
    if (result.getEntities() != null) {
      HighlightCarry.apply(result.getEntities(), explain);
    }
    return result;
  }

  @VisibleForTesting
  @Nonnull
  SearchResult finishTemplateHybrid(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nonnull SearchResult result,
      int from,
      @Nullable Integer size,
      int hybridFetchSize) {
    if (hybridFetchSize == 0) {
      return result;
    }
    ResolvedSearchTemplate resolved = opContext.getSearchContext().getResolvedSearchTemplate();
    // An explicit template kNN section is reranked by CachingEntitySearchService. It takes
    // precedence over the default entity hybrid reranker, but still receives the requested page.
    return resolved != null && resolved.isKnnApplied()
        ? sliceHybridWindow(result, from, size)
        : rerankHybrid(opContext, entityNames, input, result, from, size);
  }

  /**
   * The number of keyword rows a hybrid search fetches, or 0 when the search stays keyword-only:
   * the request is count-only, hybrid read is off, the page starts past the rerank window, no rows
   * are requested, results are not sorted by relevance, the input is not a full-text query or is an
   * exact lookup, no requested entity type has vectors, or hybrid read is paused after repeated
   * failures.
   */
  private int hybridFetchSize(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    // A count has no hits to rerank. The lexical query and facets still run.
    if (size != null && size == 0) {
      return 0;
    }
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
        || trimmed.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)
        || isExactLookup(trimmed)) {
      return 0;
    }
    final int fetchSize = Math.max(HYBRID_RERANK_WINDOW, from + pageSize);
    // A fetch above the result limit would be cut short, or rejected in strict mode
    if (fetchSize > searchServiceConfig.getLimit().getResults().getMax()
        || hybridSearchResultReranker.vectorEntityNames(opContext, entityNames).isEmpty()) {
      return 0;
    }
    if (isHybridPaused()) {
      countHybrid(opContext, "hybridReadSkipped");
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
    Future<Optional<List<SearchEntity>>> rerank = null;
    final Set<String> vectorEntityNames =
        hybridSearchResultReranker.vectorEntityNames(opContext, entityNames);
    final long windowVectorRows =
        rows.subList(0, windowEnd).stream()
            .map(SearchEntity::getEntity)
            .filter(urn -> urn != null && vectorEntityNames.contains(urn.getEntityType()))
            .distinct()
            .count();
    // Fewer than two rows of an entity type with vectors cannot trade positions, so no embedding or
    // kNN call
    if (windowVectorRows >= 2 && isHybridPaused()) {
      // Hybrid read paused after this search fetched its window: it keeps the keyword ranking
      countHybrid(opContext, "hybridReadSkipped");
    } else if (windowVectorRows >= 2) {
      final long startNanos = System.nanoTime();
      final long deadlineNanos = startNanos + TimeUnit.MILLISECONDS.toNanos(hybridTimeoutMillis);
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
        final Optional<List<SearchEntity>> reranked =
            rerank.get(hybridTimeoutMillis, TimeUnit.MILLISECONDS);
        // Also a rerank that found no vectors
        recordHybridSuccess(startNanos);
        if (reranked.isPresent()) {
          ranked = new ArrayList<>(reranked.get());
          ranked.addAll(rows.subList(windowEnd, rows.size()));
          countHybrid(opContext, "hybridReadApplied");
        }
      } catch (RejectedExecutionException e) {
        // A rejected search waited for nothing and says nothing about the provider, so a burst of
        // searches does not pause hybrid read; searches that do wait and fail count instead
        countHybrid(opContext, "hybridReadRejected");
      } catch (TimeoutException e) {
        // The interrupt ends a call blocked in an interruptible wait, and a queued call does not
        // start
        rerank.cancel(true);
        countHybridTimeout(opContext);
        recordHybridFailure(startNanos);
      } catch (InterruptedException e) {
        // The search itself was interrupted, which says nothing about the provider, so it does not
        // count toward the pause
        rerank.cancel(true);
        Thread.currentThread().interrupt();
        countHybrid(opContext, "hybridReadFailed");
      } catch (Exception e) {
        final Throwable cause =
            e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        recordHybridFailure(startNanos);
        if (System.nanoTime() - deadlineNanos >= 0) {
          // The embedding or kNN call gave up at the deadline, just as the search did
          countHybridTimeout(opContext);
        } else {
          countHybrid(opContext, "hybridReadFailed");
          // One line per failed search; the stack trace only at debug, so an outage does not
          // flood logs
          log.warn("Hybrid read failed; serving the keyword ranking: {}", cause.toString());
        }
        log.debug("Hybrid read failure", cause);
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

  @Nonnull
  private SearchResult sliceHybridWindow(
      @Nonnull SearchResult result, int from, @Nullable Integer size) {
    final List<SearchEntity> rows =
        result.getEntities() == null ? List.of() : new ArrayList<>(result.getEntities());
    final int pageSize = ConfigUtils.applyLimit(searchServiceConfig, size);
    final int pageStart = Math.min(from, rows.size());
    final int pageEnd = Math.min(from + pageSize, rows.size());
    return result
        .setEntities(new SearchEntityArray(rows.subList(pageStart, pageEnd)))
        .setFrom(from)
        .setPageSize(pageSize);
  }

  /**
   * Counts a failed search toward the pause, and pauses hybrid read once {@link
   * #HYBRID_FAILURES_BEFORE_PAUSE} have failed in a row. Only a search that started after the last
   * counted failure counts: searches that run at the same time often share one failed call, such as
   * a results page and its facets embedding the same query once, and a search still in flight when
   * a pause begins says nothing new. Synchronized, so no failure lands between another one's count
   * and the pause it starts.
   */
  private synchronized void recordHybridFailure(final long searchStartNanos) {
    if (isHybridPaused() || searchStartNanos - hybridLastFailureNanos.get() <= 0) {
      return;
    }
    hybridLastFailureNanos.set(System.nanoTime());
    if (hybridFailures.incrementAndGet() >= HYBRID_FAILURES_BEFORE_PAUSE) {
      hybridFailures.set(0);
      hybridPausedUntilNanos.set(System.nanoTime() + hybridPauseNanos);
      log.warn(
          "Hybrid read failed {} times in a row; serving the keyword ranking for {} ms.",
          HYBRID_FAILURES_BEFORE_PAUSE,
          TimeUnit.NANOSECONDS.toMillis(hybridPauseNanos));
    }
  }

  /**
   * Ends the failure streak after a completed rerank, but only if the provider answered in time
   * since both the rerank started and the last counted failure: one on a cached embedding says
   * nothing about the provider, while its kNN failures still count. Synchronized with {@link
   * #recordHybridFailure}, so a failure counted meanwhile is not erased.
   */
  private synchronized void recordHybridSuccess(final long searchStartNanos) {
    final long lastFailureNanos = hybridLastFailureNanos.get();
    if (hybridSearchResultReranker.providerSucceededSince(
        lastFailureNanos - searchStartNanos > 0 ? lastFailureNanos : searchStartNanos)) {
      hybridFailures.set(0);
    }
  }

  private boolean isHybridPaused() {
    return System.nanoTime() - hybridPausedUntilNanos.get() < 0;
  }

  @VisibleForTesting
  void setHybridPauseMillis(long millis) {
    hybridPauseNanos = TimeUnit.MILLISECONDS.toNanos(millis);
  }

  @VisibleForTesting
  void setHybridTimeoutMillis(long millis) {
    hybridTimeoutMillis = millis;
  }

  private void countHybridTimeout(@Nonnull OperationContext opContext) {
    countHybrid(opContext, "hybridReadTimeout");
    log.warn("Hybrid read took over {} ms; serving the keyword ranking.", hybridTimeoutMillis);
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
        || isExactLookup(trimmed)) {
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
      return SearchTookMetrics.time(
          opContext,
          SearchOperation.ENTITY_SEARCH,
          () ->
              searchClient(opContext, searchRequest)
                  .search(opContext, searchRequest, RequestOptions.DEFAULT));
    }
    SearchResponse lightResponse;
    searchRequest.source().query(lightSourceQuery);
    try {
      lightResponse =
          SearchTookMetrics.time(
              opContext,
              SearchOperation.ENTITY_SEARCH,
              () ->
                  searchClient(opContext, searchRequest)
                      .search(opContext, searchRequest, RequestOptions.DEFAULT));
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
    return SearchTookMetrics.time(
        opContext,
        SearchOperation.ENTITY_SEARCH,
        () ->
            searchClient(opContext, searchRequest)
                .search(opContext, searchRequest, RequestOptions.DEFAULT));
  }

  /**
   * Whether the light query's empty result stands: fuzzy expansion only adds noise for ID or hash
   * lookups and for long names that the light query did not find.
   */
  @VisibleForTesting
  static boolean skipsFullQuery(@Nonnull String input) {
    return SearchTemplateQueryShape.stopOnEmptyLight(input);
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
    if (sortCriteria == null || sortCriteria.isEmpty()) {
      return true;
    }
    return sortCriteria.stream().allMatch(ESSearchDAO::isScoreCriterion);
  }

  private static boolean isScoreCriterion(@Nonnull SortCriterion criterion) {
    String field = criterion.getField();
    return ("_score".equals(field) || "score".equals(field))
        && criterion.getOrder() != com.linkedin.metadata.query.filter.SortOrder.ASCENDING;
  }

  /** Whether the filter targets a column. Shared with the template light-query gate. */
  @VisibleForTesting
  static boolean hasColumnNameFilter(@Nullable final Filter filter) {
    return SearchTemplateLight.hasColumnNameFilter(filter);
  }

  /**
   * Returns true for exact lookups: a query wrapped in double or single quotes, or a URN or storage
   * path. Neither the light query nor the vectors have anything to add to them.
   */
  private static boolean isExactLookup(@Nonnull final String trimmedQuery) {
    return (trimmedQuery.startsWith("\"") && trimmedQuery.endsWith("\""))
        || (trimmedQuery.startsWith("'") && trimmedQuery.endsWith("'"))
        || QueryUnderstanding.understand(trimmedQuery) == QueryIntent.IDENTITY;
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
    if (templateReads(opContext)) {
      return searchOnce(
          opContext, List.of(entityName), "*", filters, sortCriteria, from, size, List.of());
    }
    shadowTemplates(opContext, List.of(entityName), "*", false);
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
    if (EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex())) {
      if (query.isBlank()) {
        return emptyAutocomplete(query);
      }
      try {
        SearchRequest searchRequest =
            buildV3AutocompleteRequest(opContext, entityName, query, field, requestParams, limit);
        SearchResponse searchResponse =
            SearchTookMetrics.time(
                opContext,
                SearchOperation.ENTITY_AUTOCOMPLETE,
                () ->
                    searchClient(opContext, searchRequest)
                        .search(opContext, searchRequest, RequestOptions.DEFAULT));
        return extractV3Autocomplete(opContext, searchResponse, query);
      } catch (Exception e) {
        log.error("Auto complete query failed:" + e.getMessage());
        throw new ESQueryException("Auto complete query failed:", e);
      }
    }
    try {
      Pair<SearchRequest, AutocompleteRequestHandler> searchRequestAndBuilder =
          buildAutocompleteRequest(opContext, entityName, query, field, requestParams, limit);
      SearchResponse searchResponse =
          SearchTookMetrics.time(
              opContext,
              SearchOperation.ENTITY_AUTOCOMPLETE,
              () ->
                  searchClient(opContext, searchRequestAndBuilder.getLeft())
                      .search(
                          opContext, searchRequestAndBuilder.getLeft(), RequestOptions.DEFAULT));
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
   * Renders {@code urn:li:searchTemplate:autocomplete} for one entity. The template owns the prefix
   * clause. This request sets the field, the limit, the caller and authorization filters, and skips
   * aggregations and the total-hit count. It does not follow the search-surface rollout.
   */
  @VisibleForTesting
  public SearchRequest buildV3AutocompleteRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    Map<String, Object> context = new HashMap<>();
    context.put(
        SearchTemplateContextVariables.PREFIX_FIELD, autocompletePrefixField(entitySpec, field));
    context.put(SearchTemplateContextVariables.UNQUOTED, query);
    context.put(
        SearchTemplateContextVariables.PREFIX_MAX_EXPANSIONS,
        SearchClauseBudget.prefixExpansions(query));
    QueryBuilder prefix =
        SearchTemplateJson.parseQuery(
            SearchTemplateMustache.render(autocompleteQuery(opContext), context));
    BoolQueryBuilder filterQuery =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getFilterQuery(opContext, requestParams);
    SearchSourceBuilder source = new SearchSourceBuilder();
    source.size(ConfigUtils.applyLimit(searchServiceConfig, limit));
    source.trackTotalHits(false);
    source.fetchSource(new String[] {"urn", "name"}, null);
    source.query(QueryBuilders.boolQuery().must(prefix).filter(filterQuery));
    SearchRequest request = new SearchRequest();
    request.source(source);
    request.indices(entityIndexName(opContext, entityName));
    return request;
  }

  @Nonnull
  private String autocompleteQuery(@Nonnull OperationContext opContext) {
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    if (coordinator != null) {
      MergedSearchTemplate template =
          coordinator.load(opContext, Constants.SEARCH_TEMPLATE_AUTOCOMPLETE_URN);
      if (template.getQuery() != null && !template.getQuery().isBlank()) {
        return template.getQuery();
      }
    }
    SearchTemplateProperties shipped =
        SearchTemplateBootstrap.get(Constants.SEARCH_TEMPLATE_AUTOCOMPLETE_URN);
    if (shipped == null || !shipped.hasQuery() || shipped.getQuery() == null) {
      throw new SearchTemplateException(
          "Search template not found: " + Constants.SEARCH_TEMPLATE_AUTOCOMPLETE_URN);
    }
    return shipped.getQuery();
  }

  @Nonnull
  private static String autocompletePrefixField(
      @Nonnull EntitySpec entitySpec, @Nullable String field) {
    if (field == null || field.isBlank()) {
      return "name" + V3SearchFields.TEXT_SUFFIX;
    }
    SearchFieldResolver resolver = new SearchFieldResolver(List.of(entitySpec));
    if (!resolver.supports(field, SearchFieldResolver.Intent.FULL_TEXT)) {
      return resolver.exactPath(field, SearchFieldResolver.Intent.EXACT);
    }
    SearchFieldResolver.Field resolved =
        resolver.resolve(field, SearchFieldResolver.Intent.FULL_TEXT);
    if (resolved.getSubfields().contains(V3SearchFields.TEXT)) {
      return resolved.getPath() + V3SearchFields.TEXT_SUFFIX;
    }
    if (resolved.getSubfields().contains(V3SearchFields.STEMMED)) {
      return resolved.getPath() + V3SearchFields.STEMMED_SUFFIX;
    }
    return resolved.getPath();
  }

  @Nonnull
  private static AutoCompleteResult emptyAutocomplete(@Nonnull String query) {
    return new AutoCompleteResult()
        .setQuery(query)
        .setSuggestions(new StringArray())
        .setEntities(new AutoCompleteEntityArray());
  }

  @Nonnull
  private static AutoCompleteResult extractV3Autocomplete(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nonnull String query) {
    List<String> suggestions = new ArrayList<>();
    List<AutoCompleteEntity> entities = new ArrayList<>();
    for (SearchHit hit : searchResponse.getHits()) {
      Map<String, Object> source = hit.getSourceAsMap();
      if (source == null) {
        continue;
      }
      Object urnValue = source.get("urn");
      if (!(urnValue instanceof String urnText) || urnText.isBlank()) {
        continue;
      }
      Urn urn;
      try {
        urn = UrnUtils.getUrn(urnText);
      } catch (Exception e) {
        log.warn("Failed to create urn {}", urnText);
        continue;
      }
      if (ESAccessControlUtil.restrictUrn(opContext, urn)) {
        continue;
      }
      suggestions.add(autocompleteSuggestion(source, urn));
      entities.add(new AutoCompleteEntity().setUrn(urn));
    }
    return new AutoCompleteResult()
        .setQuery(query)
        .setSuggestions(new StringArray(suggestions))
        .setEntities(new AutoCompleteEntityArray(entities));
  }

  @Nonnull
  private static String autocompleteSuggestion(
      @Nonnull Map<String, Object> source, @Nonnull Urn urn) {
    Object name = source.get("name");
    if (name instanceof String text && !text.isBlank()) {
      return text;
    }
    if (name instanceof List<?> values) {
      for (Object value : values) {
        if (value instanceof String text && !text.isBlank()) {
          return text;
        }
      }
    }
    return urn.getEntityKey().size() == 1 ? urn.getEntityKey().get(0) : urn.getId();
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
                SearchTookMetrics.time(
                    opContext,
                    SearchOperation.ENTITY_AGGREGATE_BY_VALUE,
                    () ->
                        searchClient(opContext, searchRequest)
                            .search(opContext, searchRequest, RequestOptions.DEFAULT));
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
    List<String> names = entitySpecs.stream().map(EntitySpec::getName).toList();
    if (templateReads(opContext)) {
      // The template supplies the lexical query and its authorization filters. The terms
      // aggregation, its size cap, and the zero-hit request stay the Java aggregation above.
      searchRequest.source().query(renderedStarQuery(opContext, names, requestParams));
    } else {
      shadowTemplates(opContext, names, "*", false);
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
                  SearchTookMetrics.time(
                      opContext,
                      SearchOperation.INCIDENT_STATS,
                      () ->
                          searchClient(opContext, searchRequest)
                              .search(opContext, searchRequest, RequestOptions.DEFAULT));
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
    if (templateReads(opContext)) {
      return scrollWithTemplates(
          opContext, entities, input, postFilters, sortCriteria, scrollId, keepAlive, size);
    }

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
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nullable String jsonQuery,
      @Nonnull SearchOperation operation) {
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

                return SearchTookMetrics.time(
                    opContext,
                    operation,
                    () ->
                        SearchClients.forIndex(opContext, searchConfiguration, resolvedIndex)
                            .search(opContext, searchRequest, RequestOptions.DEFAULT));
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
                EntityFilter.applyEntityTypeFilter(
                    query,
                    List.of(entry.getValue().getName()),
                    searchConfiguration.getEntityIndex());

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(query);

                SearchRequest searchRequest = new SearchRequest(indexName);
                searchRequest.source(searchSourceBuilder);

                return Map.entry(
                    entry.getKey(),
                    SearchTookMetrics.time(
                        opContext,
                        SearchOperation.ENTITY_SEARCH,
                        () ->
                            searchClient(opContext, searchRequest)
                                .search(opContext, searchRequest, RequestOptions.DEFAULT)));
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
          templateReads(opContext)
              ? templateExplainQuery(
                  opContext,
                  query,
                  entityName,
                  postFilters,
                  sortCriteria,
                  scrollId,
                  keepAlive,
                  size,
                  facets)
              : servedQuery(
                  opContext,
                  explainRequest.index(),
                  searchRequest.getLeft().source().query(),
                  // Scroll (a scroll id or a keep-alive) and searches without hits never run the
                  // light query
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
   * The template query a search or scroll for the same input would serve. Scroll renders the routed
   * seed and does not walk {@code next}. A search walks the ladder, including a page of size zero,
   * which fails the light page-size select and continues.
   */
  @Nonnull
  private QueryBuilder templateExplainQuery(
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
        SearchTookMetrics.time(
            opContext,
            SearchOperation.ENTITY_SEARCH,
            () ->
                searchClient(opContext, countRequest)
                    .search(opContext, countRequest, RequestOptions.DEFAULT));
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
        : EntityFilter.toEntityFilter(opContext, filter);
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

  private record SearchExecution<T>(
      @Nonnull T page, boolean shardFailed, @Nullable RenderedSearchTemplate renderedTemplate) {}

  private record PreparedRequestComponents(
      @Nonnull PreparedSearchRequest prepared,
      @Nullable Filter filter,
      @Nonnull List<EntitySpec> entitySpecs) {}

  /** The context {@code armServed} updated, plus the page that search returned. */
  private record BoundSearch(@Nonnull OperationContext context, @Nonnull SearchResult result) {}

  @Nonnull
  private BoundSearch searchWithTemplates(
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
    coordinator.armServed(bound, isRelevanceSort(sortCriteria) ? served.urn() : null);
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
  private SearchResult searchOnce(
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

  @VisibleForTesting
  public SearchRequest buildFilterRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    if (templateReads(opContext)) {
      return prepareTemplateSearch(
              opContext,
              List.of(entityName),
              "*",
              filters,
              sortCriteria,
              from,
              size,
              List.of(),
              null)
          .prepared()
          .request();
    }
    shadowTemplates(opContext, List.of(entityName), "*", false);
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    Filter transformedFilters =
        transformFilter(opContext, filters, opContext.getSearchContext().getIndexConvention());
    SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
            .getFilterRequest(opContext, transformedFilters, sortCriteria, from, size);
    searchRequest.indices(entityIndexName(opContext, entityName));
    return searchRequest;
  }

  @VisibleForTesting
  public CountRequest buildDocCountRequest(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    QueryBuilder query;
    if (templateReads(opContext)) {
      query = renderedStarQuery(opContext, List.of(entityName), filter);
    } else {
      shadowTemplates(opContext, List.of(entityName), "*", false);
      EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
      query =
          SearchRequestHandler.getFilterQuery(
              opContext,
              List.of(entityName),
              filter,
              entitySpec.getSearchableFieldTypes(),
              queryFilterRewriteChain,
              searchConfiguration.getEntityIndex());
    }
    return new CountRequest(entityIndexName(opContext, entityName)).query(query);
  }

  @Nonnull
  private ScrollResult scrollWithTemplates(
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
    PreparedSearchRequest prepared =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
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
  private PreparedRequestComponents prepareTemplateSearch(
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
        transformFilter(opContext, postFilters, opContext.getSearchContext().getIndexConvention());
    PreparedSearchRequest prepared =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfig)
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
    prepared.request().indices(entityIndexNames(opContext, entityNames));
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
          searchClient(opContext, searchRequest)
              .search(opContext, searchRequest, RequestOptions.DEFAULT);
      boolean shardFailed = searchResponse.getFailedShards() > 0 || searchResponse.isTimedOut();
      SearchResult page =
          transformIndexIntoEntityName(
              opContext,
              opContext.getSearchContext().getIndexConvention(),
              SearchRequestHandler.getBuilder(
                      opContext,
                      entitySpecs,
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
          searchClient(opContext, searchRequest)
              .search(opContext, searchRequest, RequestOptions.DEFAULT);
      ScrollResult page =
          transformIndexIntoEntityName(
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
                      size,
                      pointInTimeCreationEnabled,
                      template));
      return new SearchExecution<>(
          page, searchResponse.getFailedShards() > 0 || searchResponse.isTimedOut(), template);
    } catch (Exception e) {
      log.error("Search query failed: {}", searchRequest, e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private boolean templateReads(@Nonnull OperationContext opContext) {
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    return coordinator != null && coordinator.executes(opContext);
  }

  /**
   * Renders the template for a path whose Java request is not built by {@code SearchQueryBuilder}.
   * The rendered body is not attached to the request.
   */
  private void shadowTemplates(
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
  private QueryBuilder renderedStarQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter) {
    return prepareTemplateSearch(opContext, entityNames, "*", filter, null, 0, 0, List.of(), null)
        .prepared()
        .request()
        .source()
        .query();
  }

  /**
   * Painless body of the field-value multiplier. {@code field_value_factor} cannot subtract {@code
   * inputMin}, and a constant weight of {@code -slope * inputMin} is negative whenever {@code
   * inputMin * maxMultiplier > inputMax}. A script returns the whole multiplier, which stays at
   * least 1, so a negative stored value cannot fail the shard.
   */
  static final String FIELD_VALUE_MULTIPLIER_SCRIPT =
      "if (doc[params.field].size() == 0) { return 1.0; }"
          + " double v = ((Number) doc[params.field].get(0)).doubleValue();"
          + " for (int i = 1; i < doc[params.field].size(); i++) {"
          + "   double next = ((Number) doc[params.field].get(i)).doubleValue();"
          + "   if (next < v) { v = next; }"
          + " }"
          + " double min = ((Number) params.min).doubleValue();"
          + " double max = ((Number) params.max).doubleValue();"
          + " if (v <= min) { return 1.0; }"
          + " if (v >= max) { return ((Number) params.cap).doubleValue(); }"
          + " double slope = ((Number) params.slope).doubleValue();"
          + " return 1.0 + slope * (v - min);";

  /**
   * Wraps {@code originalQuery} so each document's score is multiplied by 1 at or below {@code
   * inputMin}, by {@code maxMultiplier} at or above {@code inputMax}, and by {@code 1 + slope *
   * (value - inputMin)} between them. A missing field stays at 1. A multi-valued field uses its
   * smallest value. Empty when the range cannot boost anything.
   */
  @VisibleForTesting
  public static Optional<QueryBuilder> structuredPropertyScoreBrowseQuery(
      @Nonnull QueryBuilder originalQuery,
      @Nonnull String field,
      double inputMin,
      double inputMax,
      double maxMultiplier) {
    if (!Double.isFinite(inputMin)
        || !Double.isFinite(inputMax)
        || !Double.isFinite(maxMultiplier)
        || !(inputMax > inputMin)
        || !(maxMultiplier > 1.0)) {
      return Optional.empty();
    }
    double slope = (maxMultiplier - 1.0) / (inputMax - inputMin);
    return Optional.of(
        QueryBuilders.functionScoreQuery(
                originalQuery,
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                  new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                      QueryBuilders.existsQuery(field),
                      ScoreFunctionBuilders.scriptFunction(
                          new Script(
                              ScriptType.INLINE,
                              "painless",
                              FIELD_VALUE_MULTIPLIER_SCRIPT,
                              Map.of(
                                  "field",
                                  field,
                                  "min",
                                  inputMin,
                                  "max",
                                  inputMax,
                                  "slope",
                                  slope,
                                  "cap",
                                  maxMultiplier))))
                })
            .scoreMode(FunctionScoreQuery.ScoreMode.SUM)
            .boostMode(CombineFunction.MULTIPLY));
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
