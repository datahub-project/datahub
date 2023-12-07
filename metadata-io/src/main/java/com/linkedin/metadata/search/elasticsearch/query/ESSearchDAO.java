package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.registry.template.util.TemplateUtil.*;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.config.search.SearchConfiguration;
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
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
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
import lombok.extern.slf4j.Slf4j;
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
public class ESSearchDAO {
  private static final NamedXContentRegistry X_CONTENT_REGISTRY;

  static {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
    X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
  }

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;
  private final boolean pointInTimeCreationEnabled;
  private final String elasticSearchImplementation;
  @Nonnull private final SearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;

  public long docCount(@Nonnull String entityName) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    CountRequest countRequest =
        new CountRequest(indexConvention.getIndexName(entitySpec))
            .query(SearchRequestHandler.getFilterQuery(null));
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "docCount").time()) {
      return client.count(countRequest, RequestOptions.DEFAULT).getCount();
    } catch (IOException e) {
      log.error("Count query failed:" + e.getMessage());
      throw new ESQueryException("Count query failed:", e);
    }
  }

  @Nonnull
  @WithSpan
  private SearchResult executeAndExtract(
      @Nonnull List<EntitySpec> entitySpec,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      int from,
      int size) {
    long id = System.currentTimeMillis();
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "executeAndExtract_search").time()) {
      log.debug("Executing request {}: {}", id, searchRequest);
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return transformIndexIntoEntityName(
          SearchRequestHandler.getBuilder(
                  entitySpec, searchConfiguration, customSearchConfiguration)
              .extractResult(searchResponse, filter, from, size));
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    } finally {
      log.debug("Returning from request {}.", id);
    }
  }

  private String transformIndexToken(String name, int entityTypeIdx) {
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
      @Nonnull AggregationMetadata aggMeta, int entityTypeIdx) {
    if (entityTypeIdx >= 0) {
      aggMeta.setAggregations(
          new LongMap(
              aggMeta.getAggregations().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          entry -> transformIndexToken(entry.getKey(), entityTypeIdx),
                          Map.Entry::getValue))));
      aggMeta.setFilterValues(
          new FilterValueArray(
              aggMeta.getFilterValues().stream()
                  .map(
                      filterValue ->
                          filterValue.setValue(
                              transformIndexToken(filterValue.getValue(), entityTypeIdx)))
                  .collect(Collectors.toList())));
    }
    return aggMeta;
  }

  @VisibleForTesting
  public SearchResult transformIndexIntoEntityName(SearchResult result) {
    return result.setMetadata(
        result
            .getMetadata()
            .setAggregations(transformIndexIntoEntityName(result.getMetadata().getAggregations())));
  }

  private ScrollResult transformIndexIntoEntityName(ScrollResult result) {
    return result.setMetadata(
        result
            .getMetadata()
            .setAggregations(transformIndexIntoEntityName(result.getMetadata().getAggregations())));
  }

  private AggregationMetadataArray transformIndexIntoEntityName(AggregationMetadataArray aggArray) {
    List<AggregationMetadata> newAggs = new ArrayList<>();
    for (AggregationMetadata aggMeta : aggArray) {
      List<String> aggregateFacets = List.of(aggMeta.getName().split(AGGREGATION_SEPARATOR_CHAR));
      int entityTypeIdx = aggregateFacets.indexOf(INDEX_VIRTUAL_FIELD);
      newAggs.add(transformAggregationMetadata(aggMeta, entityTypeIdx));
    }
    return new AggregationMetadataArray(newAggs);
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeAndExtract(
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size) {
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "executeAndExtract_scroll").time()) {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return transformIndexIntoEntityName(
          SearchRequestHandler.getBuilder(
                  entitySpecs, searchConfiguration, customSearchConfiguration)
              .extractScrollResult(
                  searchResponse, filter, scrollId, keepAlive, size, supportsPointInTime()));
    } catch (Exception e) {
      log.error("Search query failed: {}", searchRequest, e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param searchFlags Structured or full text search modes, plus other misc options
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public SearchResult search(
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size,
      @Nullable SearchFlags searchFlags,
      @Nullable List<String> facets) {
    final String finalInput = input.isEmpty() ? "*" : input;
    Timer.Context searchRequestTimer = MetricUtils.timer(this.getClass(), "searchRequest").time();
    List<EntitySpec> entitySpecs =
        entityNames.stream().map(entityRegistry::getEntitySpec).collect(Collectors.toList());
    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);
    // Step 1: construct the query
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(entitySpecs, searchConfiguration, customSearchConfiguration)
            .getSearchRequest(
                finalInput, transformedFilters, sortCriterion, from, size, searchFlags, facets);
    searchRequest.indices(
        entityNames.stream().map(indexConvention::getEntityIndexName).toArray(String[]::new));
    searchRequestTimer.stop();
    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(entitySpecs, searchRequest, transformedFilters, from, size);
  }

  /**
   * Gets a list of documents after applying the input filters.
   *
   * @param filters the request map with fields and values to be applied as filters to the search
   *     query
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size number of search hits to return
   * @return a {@link SearchResult} that contains a list of filtered documents and related search
   *     result metadata
   */
  @Nonnull
  public SearchResult filter(
      @Nonnull String entityName,
      @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Filter transformedFilters = transformFilterForEntities(filters, indexConvention);
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(entitySpec, searchConfiguration, customSearchConfiguration)
            .getFilterRequest(transformedFilters, sortCriterion, from, size);

    searchRequest.indices(indexConvention.getIndexName(entitySpec));
    return executeAndExtract(List.of(entitySpec), searchRequest, transformedFilters, from, size);
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
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      int limit) {
    try {
      EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
      AutocompleteRequestHandler builder = AutocompleteRequestHandler.getBuilder(entitySpec);
      SearchRequest req =
          builder.getSearchRequest(
              query, field, transformFilterForEntities(requestParams, indexConvention), limit);
      req.indices(indexConvention.getIndexName(entitySpec));
      SearchResponse searchResponse = client.search(req, RequestOptions.DEFAULT);
      return builder.extractResult(searchResponse, query);
    } catch (Exception e) {
      log.error("Auto complete query failed:" + e.getMessage());
      throw new ESQueryException("Auto complete query failed:", e);
    }
  }

  /**
   * Returns number of documents per field value given the field and filters
   *
   * @param entityName name of the entity, if null, aggregates over all entities
   * @param field the field name for aggregate
   * @param requestParams filters to apply before aggregating
   * @param limit the number of aggregations to return
   * @return
   */
  @Nonnull
  public Map<String, Long> aggregateByValue(
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      int limit) {
    final SearchRequest searchRequest =
        SearchRequestHandler.getAggregationRequest(
            field, transformFilterForEntities(requestParams, indexConvention), limit);
    if (entityNames == null) {
      String indexName = indexConvention.getAllEntityIndicesPattern();
      searchRequest.indices(indexName);
    } else {
      Stream<String> stream =
          entityNames.stream()
              .map(entityRegistry::getEntitySpec)
              .map(indexConvention::getIndexName);
      searchRequest.indices(stream.toArray(String[]::new));
    }

    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "aggregateByValue_search").time()) {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return SearchRequestHandler.extractTermAggregations(searchResponse, field);
    } catch (Exception e) {
      log.error("Aggregation query failed", e);
      throw new ESQueryException("Aggregation query failed:", e);
    }
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll Id to convert to a PIT ID and Sort array to pass to ElasticSearch
   * @param keepAlive string representation of the time to keep a point in time alive
   * @param size the number of search hits to return
   * @return a {@link ScrollResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  public ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size,
      SearchFlags searchFlags) {
    final String finalInput = input.isEmpty() ? "*" : input;
    String[] indexArray =
        entities.stream().map(indexConvention::getEntityIndexName).toArray(String[]::new);
    Timer.Context scrollRequestTimer = MetricUtils.timer(this.getClass(), "scrollRequest").time();
    List<EntitySpec> entitySpecs =
        entities.stream().map(entityRegistry::getEntitySpec).collect(Collectors.toList());
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

    Filter transformedFilters = transformFilterForEntities(postFilters, indexConvention);
    // Step 1: construct the query
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(entitySpecs, searchConfiguration, customSearchConfiguration)
            .getSearchRequest(
                finalInput,
                transformedFilters,
                sortCriterion,
                sort,
                pitId,
                keepAlive,
                size,
                searchFlags);

    // PIT specifies indices in creation so it doesn't support specifying indices on the request, so
    // we only specify if not using PIT
    if (!supportsPointInTime()) {
      searchRequest.indices(indexArray);
    }

    scrollRequestTimer.stop();
    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(
        entitySpecs, searchRequest, transformedFilters, scrollId, keepAlive, size);
  }

  public Optional<SearchResponse> raw(@Nonnull String indexName, @Nullable String jsonQuery) {
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
                    new SearchRequest(indexConvention.getIndexName(indexName));
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
}
