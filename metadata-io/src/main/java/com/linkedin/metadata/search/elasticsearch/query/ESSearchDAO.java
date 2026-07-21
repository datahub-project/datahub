package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.X_CONTENT_REGISTRY;
import static com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder.URN_FIELD;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.search.utils.UrnExtractionUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
@RequiredArgsConstructor
@Accessors(chain = true)
public class ESSearchDAO {

  static final String GROUP_BUCKETS_AGG = "by_group";
  static final String LATEST_TOP_HITS_AGG = "latest";
  private static final int MAX_GROUP_VALUES = 1000;

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
   * Resolves the latest document and total count for each value of {@code groupField} in a single
   * ES query (terms aggregation + top_hits).
   *
   * <p>Each returned {@link SearchResult} has {@code from=0}, {@code pageSize=1}, {@code
   * numEntities} equal to the group's document count, and at most one entity (the latest by {@code
   * sortCriteria}). Every input group value is present (empty result when the group has no docs).
   *
   * <p>Falls back to per-value {@link #filter} only when group cardinality exceeds {@link
   * #MAX_GROUP_VALUES}. Aggregation query failures throw {@link ESQueryException}.
   */
  @Nonnull
  public Map<String, SearchResult> searchLatestPerGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String groupField,
      @Nonnull Collection<String> groupValues,
      @Nullable List<SortCriterion> sortCriteria) {
    if (groupValues.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<String> distinctValues =
        groupValues.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
    if (distinctValues.isEmpty()) {
      return Collections.emptyMap();
    }
    if (distinctValues.size() > MAX_GROUP_VALUES) {
      log.warn(
          "searchLatestPerGroup falling back to per-value filter: groupValues={}",
          distinctValues.size());
      return searchLatestPerGroupFallback(
          opContext, entityName, groupField, distinctValues, sortCriteria);
    }

    return opContext.withSpan(
        "searchLatestPerGroup",
        () ->
            executeSearchLatestPerGroup(
                opContext, entityName, groupField, distinctValues, sortCriteria),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "searchLatestPerGroup"));
  }

  @Nonnull
  private Map<String, SearchResult> executeSearchLatestPerGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String groupField,
      @Nonnull List<String> distinctValues,
      @Nullable List<SortCriterion> sortCriteria) {
    final IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    final EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);

    final Filter groupFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                Collections.singletonList(
                                    buildCriterion(
                                        groupField, Condition.EQUAL, distinctValues))))));
    final Filter transformedFilters = transformFilterForEntities(groupFilter, indexConvention);

    final SearchRequestHandler requestHandler =
        SearchRequestHandler.getBuilder(
            opContext,
            entitySpec,
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            searchServiceConfig);

    // Mirror ESUtils.buildSortOrder (including urn ASC tie-break) onto the top_hits sub-agg.
    final SearchSourceBuilder sortSource = new SearchSourceBuilder();
    ESUtils.buildSortOrder(sortSource, sortCriteria, List.of(entitySpec));
    final TopHitsAggregationBuilder topHitsAgg =
        AggregationBuilders.topHits(LATEST_TOP_HITS_AGG)
            .size(1)
            .fetchSource(new String[] {URN_FIELD}, null);
    for (SortBuilder<?> sort : sortSource.sorts()) {
      topHitsAgg.sort(sort);
    }

    final String keywordField =
        ESUtils.toKeywordField(opContext, groupField, false, opContext.getAspectRetriever());
    final SearchSourceBuilder sourceBuilder =
        new SearchSourceBuilder()
            .query(requestHandler.getFilterQuery(opContext, transformedFilters))
            .size(0)
            .aggregation(
                AggregationBuilders.terms(GROUP_BUCKETS_AGG)
                    .field(keywordField)
                    .size(distinctValues.size())
                    .subAggregation(topHitsAgg));

    final SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(sourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(entitySpec));

    try {
      final SearchResponse response =
          client.search(opContext, searchRequest, RequestOptions.DEFAULT);
      return mapSearchLatestPerGroupResponse(response, distinctValues);
    } catch (Exception e) {
      log.error(
          "searchLatestPerGroup failed for entity={} groupField={} values={}",
          entityName,
          groupField,
          distinctValues.size(),
          e);
      throw new ESQueryException("searchLatestPerGroup query failed:", e);
    }
  }

  @Nonnull
  private Map<String, SearchResult> mapSearchLatestPerGroupResponse(
      @Nonnull SearchResponse searchResponse, @Nonnull List<String> distinctValues) {
    final Map<String, SearchResult> results = new LinkedHashMap<>();
    for (String groupValue : distinctValues) {
      results.put(groupValue, emptyLatestPerGroupResult());
    }
    if (searchResponse.getAggregations() == null) {
      return results;
    }
    final ParsedTerms terms = searchResponse.getAggregations().get(GROUP_BUCKETS_AGG);
    if (terms == null) {
      return results;
    }
    for (Terms.Bucket bucket : terms.getBuckets()) {
      final String groupValue = bucket.getKeyAsString();
      if (!results.containsKey(groupValue)) {
        continue;
      }
      final ParsedTopHits topHits = bucket.getAggregations().get(LATEST_TOP_HITS_AGG);
      final SearchEntityArray entities = new SearchEntityArray();
      if (topHits != null) {
        for (SearchHit hit : topHits.getHits().getHits()) {
          try {
            entities.add(
                new SearchEntity().setEntity(UrnExtractionUtils.extractUrnFromSearchHit(hit)));
          } catch (RuntimeException e) {
            log.warn(
                "Skipping invalid search hit in searchLatestPerGroup bucket {}: {}",
                groupValue,
                e.getMessage());
          }
        }
      }
      results.put(
          groupValue,
          new SearchResult()
              .setFrom(0)
              .setPageSize(1)
              .setNumEntities((int) bucket.getDocCount())
              .setEntities(entities)
              .setMetadata(new SearchResultMetadata()));
    }
    return results;
  }

  @Nonnull
  private Map<String, SearchResult> searchLatestPerGroupFallback(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String groupField,
      @Nonnull List<String> distinctValues,
      @Nullable List<SortCriterion> sortCriteria) {
    final Map<String, SearchResult> results = new LinkedHashMap<>();
    for (String value : distinctValues) {
      final Filter filters =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(
                                  Collections.singletonList(
                                      buildCriterion(groupField, Condition.EQUAL, value))))));
      final SearchResult result = filter(opContext, entityName, filters, sortCriteria, 0, 1);
      results.put(value, result != null ? result : emptyLatestPerGroupResult());
    }
    return results;
  }

  @Nonnull
  private static SearchResult emptyLatestPerGroupResult() {
    return new SearchResult()
        .setFrom(0)
        .setPageSize(1)
        .setNumEntities(0)
        .setEntities(new SearchEntityArray())
        .setMetadata(new SearchResultMetadata());
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
