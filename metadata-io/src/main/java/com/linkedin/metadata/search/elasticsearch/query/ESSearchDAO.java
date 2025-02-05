package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.*;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
@RequiredArgsConstructor
@Accessors(chain = true)
public class ESSearchDAO {
  private static final NamedXContentRegistry X_CONTENT_REGISTRY;

  static {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
    X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
  }

  private final RestHighLevelClient client;
  private final boolean pointInTimeCreationEnabled;
  private final String elasticSearchImplementation;
  @Nonnull private final SearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final boolean testLoggingEnabled;

  public ESSearchDAO(
      RestHighLevelClient client,
      boolean pointInTimeCreationEnabled,
      String elasticSearchImplementation,
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    this(
        client,
        pointInTimeCreationEnabled,
        elasticSearchImplementation,
        searchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        false);
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
      int size) {
    long id = System.currentTimeMillis();

    return opContext.withSpan(
        "executeAndExtract_search",
        () -> {
          try {
            log.debug("Executing request {}: {}", id, searchRequest);
            final SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);
            // extract results, validated against document model as well
            return transformIndexIntoEntityName(
                opContext.getSearchContext().getIndexConvention(),
                SearchRequestHandler.getBuilder(
                        opContext,
                        entitySpec,
                        searchConfiguration,
                        customSearchConfiguration,
                        queryFilterRewriteChain)
                    .extractResult(opContext, searchResponse, filter, from, size));
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
      int size) {
    return opContext.withSpan(
        "executeAndExtract_scroll",
        () -> {
          try {
            final SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);
            // extract results, validated against document model as well
            return transformIndexIntoEntityName(
                opContext.getSearchContext().getIndexConvention(),
                SearchRequestHandler.getBuilder(
                        opContext,
                        entitySpecs,
                        searchConfiguration,
                        customSearchConfiguration,
                        queryFilterRewriteChain)
                    .extractScrollResult(
                        opContext, searchResponse, filter, keepAlive, size, supportsPointInTime()));
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
      int size,
      @Nullable List<String> facets) {
    final String finalInput = input.isEmpty() ? "*" : input;
    List<EntitySpec> entitySpecs =
        entityNames.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .collect(Collectors.toList());
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);

    // Step 1: construct the query
    final SearchRequest searchRequest =
        opContext.withSpan(
            "searchRequest",
            () ->
                SearchRequestHandler.getBuilder(
                        opContext,
                        entitySpecs,
                        searchConfiguration,
                        customSearchConfiguration,
                        queryFilterRewriteChain)
                    .getSearchRequest(
                        opContext, finalInput, transformedFilters, sortCriteria, from, size, facets)
                    .indices(
                        entityNames.stream()
                            .map(indexConvention::getEntityIndexName)
                            .toArray(String[]::new)),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(this.getClass(), "searchRequest"));

    if (testLoggingEnabled) {
      testLog(opContext.getObjectMapper(), searchRequest);
    }

    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(opContext, entitySpecs, searchRequest, transformedFilters, from, size);
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
      int size) {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    Filter transformedFilters = transformFilterForEntities(filters, indexConvention);
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain)
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
      int limit) {
    try {

      EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
      IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
      AutocompleteRequestHandler builder =
          AutocompleteRequestHandler.getBuilder(
              opContext,
              entitySpec,
              customSearchConfiguration,
              queryFilterRewriteChain,
              searchConfiguration);
      SearchRequest req =
          builder.getSearchRequest(
              opContext,
              query,
              field,
              transformFilterForEntities(requestParams, indexConvention),
              limit);
      req.indices(indexConvention.getIndexName(entitySpec));
      SearchResponse searchResponse = client.search(req, RequestOptions.DEFAULT);
      return builder.extractResult(opContext, searchResponse, query);
    } catch (Exception e) {
      log.error("Auto complete query failed:" + e.getMessage());
      throw new ESQueryException("Auto complete query failed:", e);
    }
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
      int limit) {

    List<EntitySpec> entitySpecs;
    if (entityNames == null || entityNames.isEmpty()) {
      entitySpecs = QueryUtils.getQueryByDefaultEntitySpecs(opContext.getEntityRegistry());
    } else {
      entitySpecs =
          entityNames.stream()
              .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
              .collect(Collectors.toList());
    }
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain)
            .getAggregationRequest(
                opContext,
                field,
                transformFilterForEntities(requestParams, indexConvention),
                limit);
    if (entityNames == null) {
      String indexName = indexConvention.getAllEntityIndicesPattern();
      searchRequest.indices(indexName);
    } else {
      Stream<String> stream =
          entityNames.stream()
              .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
              .map(indexConvention::getIndexName);
      searchRequest.indices(stream.toArray(String[]::new));
    }

    return opContext.withSpan(
        "aggregateByValue_search",
        () -> {
          try {
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
      int size) {
    final String finalInput = input.isEmpty() ? "*" : input;
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String[] indexArray =
        entities.stream().map(indexConvention::getEntityIndexName).toArray(String[]::new);
    List<EntitySpec> entitySpecs =
        entities.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .collect(Collectors.toList());
    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);

    final SearchRequest searchRequest =
        opContext.withSpan(
            "scrollRequest",
            () -> {
              // TODO: Align scroll and search using facets
              final SearchRequest req =
                  getScrollRequest(
                      opContext,
                      scrollId,
                      keepAlive,
                      indexArray,
                      size,
                      transformedFilters,
                      entitySpecs,
                      finalInput,
                      sortCriteria,
                      null);

              // PIT specifies indices in creation so it doesn't support specifying indices on the
              // request, so
              // we only specify if not using PIT
              if (!supportsPointInTime()) {
                req.indices(indexArray);
              }
              return req;
            },
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(this.getClass(), "scrollRequest"));

    if (testLoggingEnabled) {
      testLog(opContext.getObjectMapper(), searchRequest);
    }

    return executeAndExtract(
        opContext, entitySpecs, searchRequest, transformedFilters, keepAlive, size);
  }

  private SearchRequest getScrollRequest(
      @Nonnull OperationContext opContext,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      String[] indexArray,
      int size,
      @Nullable Filter postFilters,
      List<EntitySpec> entitySpecs,
      String finalInput,
      List<SortCriterion> sortCriteria,
      @Nullable List<String> facets) {
    String pitId = null;
    Object[] sort = null;
    if (scrollId != null) {
      SearchAfterWrapper searchAfterWrapper = SearchAfterWrapper.fromScrollId(scrollId);
      sort = searchAfterWrapper.getSort();
      if (supportsPointInTime()) {
        if (System.currentTimeMillis() + 10000 <= searchAfterWrapper.getExpirationTime()) {
          pitId = searchAfterWrapper.getPitId();
        } else if (keepAlive != null) {
          pitId = createPointInTime(indexArray, keepAlive);
        }
      }
    } else if (supportsPointInTime() && keepAlive != null) {
      pitId = createPointInTime(indexArray, keepAlive);
    }

    return SearchRequestHandler.getBuilder(
            opContext,
            entitySpecs,
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain)
        .getSearchRequest(
            opContext, finalInput, postFilters, sortCriteria, sort, pitId, keepAlive, size, facets);
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

  private boolean supportsPointInTime() {
    return pointInTimeCreationEnabled
        && ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH.equalsIgnoreCase(elasticSearchImplementation);
  }

  private String createPointInTime(String[] indexArray, String keepAlive) {
    String endPoint = String.join(",", indexArray) + "/_pit";
    Request request = new Request("POST", endPoint);
    request.addParameter("keep_alive", keepAlive);
    try {
      Response response = client.getLowLevelClient().performRequest(request);
      Map<String, Object> mappedResponse =
          OBJECT_MAPPER.readValue(response.getEntity().getContent(), new TypeReference<>() {});
      return (String) mappedResponse.get("id");
    } catch (IOException e) {
      log.error("Failed to generate PointInTime Identifier.", e);
      throw new IllegalStateException("Failed to generate PointInTime Identifier.:", e);
    }
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
      int size,
      @Nullable List<String> facets) {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);
    final String[] indexArray = new String[] {indexConvention.getEntityIndexName(entityName)};
    final String finalQuery = query.isEmpty() ? "*" : query;
    final SearchRequest searchRequest =
        getScrollRequest(
            opContext,
            scrollId,
            keepAlive,
            indexArray,
            size,
            transformedFilters,
            Collections.singletonList(entitySpec),
            finalQuery,
            sortCriteria,
            facets);
    ;

    ExplainRequest explainRequest = new ExplainRequest();
    explainRequest
        .query(searchRequest.source().query())
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
