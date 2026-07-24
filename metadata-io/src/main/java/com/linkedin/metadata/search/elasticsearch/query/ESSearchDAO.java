package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.X_CONTENT_REGISTRY;
import static com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder.URN_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
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
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.IncidentStats;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
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

  private final SearchClientShim<?> client;
  private final boolean pointInTimeCreationEnabled;
  @Nonnull private final ElasticSearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final boolean testLoggingEnabled;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;

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
            return client.count(opContext, countRequest, RequestOptions.DEFAULT).getCount();
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
          SearchResponse searchResponse = null;
          try {
            log.debug("Executing request {}: {}", id, searchRequest);
            searchResponse = client.search(opContext, searchRequest, RequestOptions.DEFAULT);
            // extract results, validated against document model as well
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
                client.search(opContext, searchRequest, RequestOptions.DEFAULT);
            // extract results, validated against document model as well
            return transformIndexIntoEntityName(
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
          client.search(opContext, searchRequestAndBuilder.getLeft(), RequestOptions.DEFAULT);
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
                client.search(opContext, searchRequest, RequestOptions.DEFAULT);
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
                  client.search(opContext, searchRequest, RequestOptions.DEFAULT);
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
        new SearchRequest(indexConvention.getEntityIndexName(Constants.INCIDENT_ENTITY_NAME));
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

    boolean hasSliceOptions = opContext.getSearchContext().getSearchFlags().hasSliceOptions();
    if (hasSliceOptions && isSliceDisabled()) {
      throw new IllegalStateException(
          "Slice options are not supported with the current ES implementation: "
              + client.getEngineType()
              + ". Please disable slice options in the search flags.");
    }

    boolean usePIT = (pointInTimeCreationEnabled || hasSliceOptions) && keepAlive != null;
    String pitId =
        usePIT
            ? ESUtils.computePointInTime(opContext, scrollId, keepAlive, client, indexArray)
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

                SearchRequest searchRequest =
                    new SearchRequest(
                        opContext.getSearchContext().getIndexConvention().getIndexName(indexName));
                searchRequest.source(searchSourceBuilder);

                return client.search(opContext, searchRequest, RequestOptions.DEFAULT);
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
                    entry.getKey(),
                    client.search(opContext, searchRequest, RequestOptions.DEFAULT));
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
      return client.explain(opContext, explainRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to explain query.", e);
      throw new IllegalStateException("Failed to explain query:", e);
    }
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
