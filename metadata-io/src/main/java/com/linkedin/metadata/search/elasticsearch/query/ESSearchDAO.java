package com.linkedin.metadata.search.elasticsearch.query;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.registry.template.util.TemplateUtil.*;
import static com.linkedin.metadata.search.utils.SearchUtils.*;

/**
 * A search DAO for Elasticsearch backend.
 */
@Slf4j
@RequiredArgsConstructor
public class ESSearchDAO {

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;
  private final boolean pointInTimeCreationEnabled;
  private final String elasticSearchImplementation;

  public long docCount(@Nonnull String entityName) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    CountRequest countRequest =
        new CountRequest(indexConvention.getIndexName(entitySpec)).query(SearchRequestHandler.getFilterQuery(null));
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "docCount").time()) {
      return client.count(countRequest, RequestOptions.DEFAULT).getCount();
    } catch (IOException e) {
      log.error("Count query failed:" + e.getMessage());
      throw new ESQueryException("Count query failed:", e);
    }
  }

  @Nonnull
  @WithSpan
  private SearchResult executeAndExtract(@Nonnull EntitySpec entitySpec, @Nonnull SearchRequest searchRequest,
      @Nullable Filter filter, int from, int size) {
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "executeAndExtract_search").time()) {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return SearchRequestHandler.getBuilder(entitySpec).extractResult(searchResponse, filter, from, size);
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Nonnull
  @WithSpan
  private ScrollResult executeAndExtract(@Nonnull List<EntitySpec> entitySpecs, @Nonnull SearchRequest searchRequest, @Nullable Filter filter,
      @Nullable String scrollId, @Nonnull String keepAlive, int size) {
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "executeAndExtract_scroll").time()) {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return SearchRequestHandler.getBuilder(entitySpecs).extractScrollResult(searchResponse, filter, scrollId, keepAlive, size, supportsPointInTime());
    } catch (Exception e) {
      if (e instanceof ElasticsearchStatusException) {
        final ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
        if (statusException.status().getStatus() == 400) {
          // Malformed query -- Could indicate bad search syntax. Return empty response.
          log.warn("Received 400 from Elasticsearch. Returning empty search response", e);
          return EMPTY_SCROLL_RESULT;
        }
      }
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param fulltext Structured or full text search modes
   * @return a {@link SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public SearchResult search(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size, boolean fulltext) {
    final String finalInput = input.isEmpty() ? "*" : input;
    Timer.Context searchRequestTimer = MetricUtils.timer(this.getClass(), "searchRequest").time();
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    // Step 1: construct the query
    final SearchRequest searchRequest = SearchRequestHandler.getBuilder(entitySpec)
        .getSearchRequest(finalInput, postFilters, sortCriterion, from, size, fulltext);
    searchRequest.indices(indexConvention.getIndexName(entitySpec));
    searchRequestTimer.stop();
    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(entitySpec, searchRequest, postFilters, from, size);
  }

  /**
   * Gets a list of documents after applying the input filters.
   *
   * @param filters the request map with fields and values to be applied as filters to the search query
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size number of search hits to return
   * @return a {@link SearchResult} that contains a list of filtered documents and related search result metadata
   */
  @Nonnull
  public SearchResult filter(@Nonnull String entityName, @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    final SearchRequest searchRequest =
        SearchRequestHandler.getBuilder(entitySpec).getFilterRequest(filters, sortCriterion, from, size);
    searchRequest.indices(indexConvention.getIndexName(entitySpec));
    return executeAndExtract(entitySpec, searchRequest, filters, from, size);
  }

  /**
   * Returns a list of suggestions given type ahead query.
   *
   * <p>The advanced auto complete can take filters and provides suggestions based on filtered context.
   *
   * @param query the type ahead query text
   * @param field the field name for the auto complete
   * @param requestParams specify the field to auto complete and the input text
   * @param limit the number of suggestions returned
   * @return A list of suggestions as string
   */
  @Nonnull
  public AutoCompleteResult autoComplete(@Nonnull String entityName, @Nonnull String query, @Nullable String field,
      @Nullable Filter requestParams, int limit) {
    try {
      EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
      AutocompleteRequestHandler builder = AutocompleteRequestHandler.getBuilder(entitySpec);
      SearchRequest req = builder.getSearchRequest(query, field, requestParams, limit);
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
  public Map<String, Long> aggregateByValue(@Nullable String entityName, @Nonnull String field,
      @Nullable Filter requestParams, int limit) {
    final SearchRequest searchRequest = SearchRequestHandler.getAggregationRequest(field, requestParams, limit);
    String indexName;
    if (entityName == null) {
      indexName = indexConvention.getAllEntityIndicesPattern();
    } else {
      EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
      indexName = indexConvention.getIndexName(entitySpec);
    }
    searchRequest.indices(indexName);

    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "aggregateByValue_search").time()) {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return SearchRequestHandler.extractTermAggregations(searchResponse, field);
    } catch (Exception e) {
      log.error("Aggregation query failed", e);
      throw new ESQueryException("Aggregation query failed:", e);
    }
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll Id to convert to a PIT ID and Sort array to pass to ElasticSearch
   * @param keepAlive string representation of the time to keep a point in time alive
   * @param size the number of search hits to return
   * @return a {@link ScrollResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public ScrollResult scroll(@Nonnull List<String> entities, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, @Nullable String scrollId, @Nonnull String keepAlive, int size, boolean fulltext) {
    final String finalInput = input.isEmpty() ? "*" : input;
    String[] indexArray = entities.stream()
        .map(indexConvention::getEntityIndexName)
        .toArray(String[]::new);
    Timer.Context scrollRequestTimer = MetricUtils.timer(this.getClass(), "scrollRequest").time();
    List<EntitySpec> entitySpecs = entities.stream()
        .map(entityRegistry::getEntitySpec)
        .collect(Collectors.toList());
    String pitId = null;
    Object[] sort = null;
    if (scrollId != null) {
      SearchAfterWrapper searchAfterWrapper = SearchAfterWrapper.fromScrollId(scrollId);
      sort = searchAfterWrapper.getSort();
      if (supportsPointInTime()) {
        if (System.currentTimeMillis() + 10000 <= searchAfterWrapper.getExpirationTime()) {
          pitId = searchAfterWrapper.getPitId();
        } else {
          pitId = createPointInTime(indexArray, keepAlive);
        }
      }
    } else if (supportsPointInTime()) {
      pitId = createPointInTime(indexArray, keepAlive);
    }

    // Step 1: construct the query
    final SearchRequest searchRequest = SearchRequestHandler.getBuilder(entitySpecs)
        .getSearchRequest(finalInput, postFilters, sortCriterion, sort, pitId, keepAlive, size, fulltext);

    // PIT specifies indices in creation so it doesn't support specifying indices on the request, so we only specify if not using PIT
    if (!supportsPointInTime()) {
      searchRequest.indices(indexArray);
    }

    scrollRequestTimer.stop();
    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(entitySpecs, searchRequest, postFilters, scrollId, keepAlive, size);
  }

  private boolean supportsPointInTime() {
    return pointInTimeCreationEnabled && ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH.equalsIgnoreCase(elasticSearchImplementation);
  }

  private String createPointInTime(String[] indexArray, String keepAlive) {
    String endPoint = String.join(",", indexArray) + "/_pit";
    Request request = new Request("POST", endPoint);
    request.addParameter("keep_alive", keepAlive);
    try {
      Response response = client.getLowLevelClient().performRequest(request);
      Map<String, Object> mappedResponse = OBJECT_MAPPER.readValue(response.getEntity().getContent(),
          new TypeReference<>() { });
      return (String) mappedResponse.get("id");
    } catch (IOException e) {
      log.error("Failed to generate PointInTime Identifier.", e);
      throw new IllegalStateException("Failed to generate PointInTime Identifier.:", e);
    }
  }
}
