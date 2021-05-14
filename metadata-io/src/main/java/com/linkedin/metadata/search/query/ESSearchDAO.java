package com.linkedin.metadata.search.query;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.MatchMetadata;
import com.linkedin.metadata.query.MatchMetadataArray;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.search.query.builder.AutocompleteQueryBuilder;
import com.linkedin.metadata.search.query.builder.FacetsAggregationBuilder;
import com.linkedin.metadata.search.query.builder.SearchHighlightsBuilder;
import com.linkedin.metadata.search.query.builder.SearchQueryBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static com.linkedin.metadata.dao.utils.SearchUtils.getQueryBuilderFromCriterion;


/**
 * A search DAO for Elasticsearch backend.
 */
@Slf4j
@RequiredArgsConstructor
public class ESSearchDAO {

  private static final String URN_FIELD = "urn";

  private final RestHighLevelClient _client;
  private final IndexConvention _indexConvention;

  @Nonnull
  private String getIndexName(@Nonnull String entityName) {
    return _indexConvention.getIndexName(SnapshotEntityRegistry.getInstance().getEntitySpec(entityName));
  }

  @Nonnull
  private SearchResultObject executeAndExtract(@Nonnull String entityName, @Nonnull SearchRequest searchRequest, int from,
      int size) {
    try {
      final SearchResponse searchResponse = _client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return extractQueryResult(entityName, searchResponse, from, size);
    } catch (Exception e) {
      log.error("Search query failed:" + e.getMessage());
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Nonnull
  public SearchResult search(
      @Nonnull String entityName,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size) {

    final SearchResultObject searchResultObject = searchInternal(entityName, input, postFilters, sortCriterion, from, size);
    return new SearchResult()
        .setEntities(new UrnArray(searchResultObject.getResultList()))
        .setMetadata(searchResultObject.getSearchResultMetadata())
        .setFrom(searchResultObject.getFrom())
        .setPageSize(searchResultObject.getPageSize())
        .setNumEntities(searchResultObject.getTotalCount());
  }

  /**
   * TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   */
  @Nonnull
  private SearchResultObject searchInternal(
      @Nonnull String entityName,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size) {
    return searchInternal(entityName, input, postFilters, sortCriterion, null, from, size);
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * <p>This method uses preference parameter to control the shard copy on which to execute the search operation.
   * The string used as preference can be a user ID or session ID for instance. This ensures that all queries of a
   * given user are always going to hit the same shards, so scores can remain more consistent across queries. Using a
   * preference value that identifies the current user or session could help optimize usage of the caches.
   *
   * <p>WARNING: using a preference parameter that is same for all queries will lead to hot spots that could
   * potentially impact latency, hence choose this parameter judiciously.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param preference controls a preference of the shard copy on which to execute the search
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link SearchResultObject} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  private SearchResultObject searchInternal(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, @Nullable String preference, int from, int size) {
    // Step 1: construct the query
    final SearchRequest req =
        constructSearchQuery(entityName, input, postFilters, sortCriterion, preference, from, size);
    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(entityName, req, from, size);
  }

  @Nonnull
  public SearchResultObject filter(@Nonnull String entityName, @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion, int from, int size) {

    final SearchRequest searchRequest = getFilteredSearchQuery(entityName, filters, sortCriterion, from, size);
    return executeAndExtract(entityName, searchRequest, from, size);
  }

  /**
   * Returns a {@link SearchRequest} given filters to be applied to search query and sort criterion to be applied to
   * search results.
   *
   * @param filters {@link Filter} list of conditions with fields and values
   * @param sortCriterion {@link SortCriterion} to be applied to the search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return {@link SearchRequest} that contains the filtered query
   */
  @Nonnull
  SearchRequest getFilteredSearchQuery(@Nonnull String entityName, @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion, int from, int size) {

    final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    if (filters != null) {
      filters.getCriteria().forEach(criterion -> {
        if (!criterion.getValue().trim().isEmpty()) {
          boolQueryBuilder.filter(getQueryBuilderFromCriterion(criterion));
        }
      });
    }
    final SearchRequest searchRequest = new SearchRequest(getIndexName(entityName));
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder);
    searchSourceBuilder.from(from).size(size);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   *
   * @param input the search input text
   * @param filter the search filter
   * @param preference controls a preference of the shard copy on which to execute the search
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a valid search request
   */
  @Nonnull
  SearchRequest constructSearchQuery(@Nonnull String entityName, @Nonnull String input, @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion, @Nullable String preference, int from, int size) {

    SearchRequest searchRequest = new SearchRequest(getIndexName(entityName));
    if (preference != null) {
      searchRequest.preference(preference);
    }
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);

    searchSourceBuilder.query(SearchQueryBuilder.getBuilder(entityName).getQuery(input));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(filter));
    FacetsAggregationBuilder.getBuilder(entityName).getAggregations(filter).forEach(searchSourceBuilder::aggregation);
    searchSourceBuilder.highlighter(SearchHighlightsBuilder.getBuilder(entityName).getHighlights());
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);

    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest.toString());
    return searchRequest;
  }

  /**
   * Extracts a list of documents from the raw search response.
   *
   * @param searchResponse the raw search response from search engine
   * @param from offset from the first result you want to fetch
   * @param size page size
   * @return collection of a list of documents and related search result metadata
   */
  @Nonnull
  public SearchResultObject extractQueryResult(@Nonnull String entityName, @Nonnull SearchResponse searchResponse, int from,
      int size) {

    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    int totalPageCount = QueryUtils.getTotalPageCount(totalCount, size);
    List<Urn> resultList = getResults(searchResponse);
    SearchResultMetadata searchResultMetadata = extractSearchResultMetadata(entityName, searchResponse);
    searchResultMetadata.setUrns(new UrnArray(resultList));

    return SearchResultObject.builder()
        .resultList(resultList)
        .searchResultMetadata(searchResultMetadata)
        .from(from)
        .pageSize(size)
        .havingMore(QueryUtils.hasMore(from, size, totalPageCount))
        .totalCount(totalCount)
        .totalPageCount(totalPageCount)
        .build();
  }

  /**
   * Gets list of urns returned in the search response
   *
   * @param searchResponse the raw search response from search engine
   * @return List of entity urns
   */
  @Nonnull
  List<Urn> getResults(@Nonnull SearchResponse searchResponse) {
    return Arrays.stream(searchResponse.getHits().getHits())
        .map(this::getUrnFromSearchHit)
        .collect(Collectors.toList());
  }

  @Nonnull
  public AutoCompleteResult autoComplete(
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      int limit) {
    try {
      SearchRequest req = constructAutoCompleteQuery(entityName, query, field, requestParams, limit);
      SearchResponse searchResponse = _client.search(req, RequestOptions.DEFAULT);
      return AutocompleteQueryBuilder.getBuilder(entityName).extractResult(searchResponse, query);
    } catch (Exception e) {
      log.error("Auto complete query failed:" + e.getMessage());
      throw new ESQueryException("Auto complete query failed:", e);
    }
  }

  @Nonnull
  public SearchRequest constructAutoCompleteQuery(@Nonnull String entityName, @Nonnull String input,
      @Nullable String field, @Nullable Filter filter, int limit) {
    SearchRequest searchRequest = new SearchRequest(getIndexName(entityName));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    AutocompleteQueryBuilder builder = AutocompleteQueryBuilder.getBuilder(entityName);
    searchSourceBuilder.size(limit);
    searchSourceBuilder.query(builder.getQuery(input, field));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(filter));
    searchSourceBuilder.highlighter(builder.getHighlights(field));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  /**
   * Extracts SearchResultMetadata section.
   *
   * @param searchResponse the raw {@link SearchResponse} as obtained from the search engine
   * @return {@link SearchResultMetadata} with aggregation and list of urns obtained from {@link SearchResponse}
   */
  @Nonnull
  SearchResultMetadata extractSearchResultMetadata(@Nonnull String entityName, @Nonnull SearchResponse searchResponse) {
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray()).setUrns(new UrnArray());

    final List<AggregationMetadata> aggregationMetadataList =
        FacetsAggregationBuilder.getBuilder(entityName).extractAggregation(searchResponse);
    if (!aggregationMetadataList.isEmpty()) {
      searchResultMetadata.setSearchResultMetadatas(new AggregationMetadataArray(aggregationMetadataList));
    }

    final List<MatchMetadata> highlightMetadataList =
        SearchHighlightsBuilder.getBuilder(entityName).extractHighlights(searchResponse);
    if (!highlightMetadataList.isEmpty()) {
      searchResultMetadata.setMatches(new MatchMetadataArray(highlightMetadataList));
    }

    return searchResultMetadata;
  }

  @Nonnull
  private Urn getUrnFromSearchHit(@Nonnull SearchHit hit) {
    try {
      return Urn.createFromString(hit.getSourceAsMap().get(URN_FIELD).toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid urn in search document " + e);
    }
  }
}
