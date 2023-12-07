package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.utils.SearchUtil.filterSoftDeletedByDefault;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.browse.BrowseResultGroup;
import com.linkedin.metadata.browse.BrowseResultGroupArray;
import com.linkedin.metadata.browse.BrowseResultGroupV2;
import com.linkedin.metadata.browse.BrowseResultGroupV2Array;
import com.linkedin.metadata.browse.BrowseResultMetadata;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

@Slf4j
@RequiredArgsConstructor
public class ESBrowseDAO {

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;
  @Nonnull private final SearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;

  private static final String BROWSE_PATH = "browsePaths";
  private static final String BROWSE_PATH_DEPTH = "browsePaths.length";
  private static final String BROWSE_PATH_V2 = "browsePathV2";
  private static final String BROWSE_PATH_V2_DEPTH = "browsePathV2.length";
  private static final String BROWSE_V2_DELIMITER = "␟";
  private static final String URN = "urn";
  private static final String REMOVED = "removed";

  private static final String GROUP_AGG = "groups";

  // Set explicit max size for grouping
  private static final int AGGREGATION_MAX_SIZE = 2000;

  @Value
  private class BrowseGroupsResult {
    List<BrowseResultGroup> groups;
    int totalGroups;
    int totalNumEntities;
  }

  @Value
  private class BrowseGroupsResultV2 {
    List<BrowseResultGroupV2> groups;
    int totalGroups;
    int totalNumEntities;
  }

  /**
   * Gets a list of groups/entities that match given browse request.
   *
   * @param entityName type of entity to query
   * @param path the path to be browsed
   * @param filters the request map with fields and values as filters
   * @param from index of the first entity located in path
   * @param size the max number of entities contained in the response
   * @return a {@link BrowseResult} that contains a list of groups/entities
   */
  @Nonnull
  public BrowseResult browse(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      int size) {
    final Map<String, String> requestMap = SearchUtils.getRequestMap(filters);

    try {
      final String indexName =
          indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));

      final SearchResponse groupsResponse;
      try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esGroupSearch").time()) {
        groupsResponse =
            client.search(
                constructGroupsSearchRequest(indexName, path, requestMap), RequestOptions.DEFAULT);
      }
      final BrowseGroupsResult browseGroupsResult =
          extractGroupsResponse(groupsResponse, path, from, size);
      final int numGroups = browseGroupsResult.getTotalGroups();

      // Based on the number of groups returned, compute the from and size to query for entities
      // Groups come before entities, so if numGroups >= from + size, we should return all groups
      // if from < numGroups < from + size, we should return a mix of groups and entities
      // if numGroups <= from, we should only return entities
      int entityFrom = Math.max(from - numGroups, 0);
      int entitySize = Math.min(Math.max(from + size - numGroups, 0), size);
      final SearchResponse entitiesResponse;
      try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esEntitiesSearch").time()) {
        entitiesResponse =
            client.search(
                constructEntitiesSearchRequest(indexName, path, requestMap, entityFrom, entitySize),
                RequestOptions.DEFAULT);
      }
      final int numEntities = (int) entitiesResponse.getHits().getTotalHits().value;
      final List<BrowseResultEntity> browseResultEntityList =
          extractEntitiesResponse(entitiesResponse, path);

      return new BrowseResult()
          .setMetadata(
              new BrowseResultMetadata()
                  .setTotalNumEntities(browseGroupsResult.getTotalNumEntities())
                  .setPath(path))
          .setEntities(new BrowseResultEntityArray(browseResultEntityList))
          .setGroups(new BrowseResultGroupArray(browseGroupsResult.getGroups()))
          .setNumEntities(numEntities)
          .setNumGroups(numGroups)
          .setNumElements(numGroups + numEntities)
          .setFrom(from)
          .setPageSize(size);
    } catch (Exception e) {
      log.error("Browse query failed: " + e.getMessage());
      throw new ESQueryException("Browse query failed: ", e);
    }
  }

  /**
   * Builds aggregations for search request.
   *
   * @param path the path which is being browsed
   * @return {@link AggregationBuilder}
   */
  @Nonnull
  private AggregationBuilder buildAggregations(@Nonnull String path) {
    final String currentLevel = ESUtils.escapeReservedCharacters(path) + "/.*";
    final String nextLevel = ESUtils.escapeReservedCharacters(path) + "/.*/.*";

    return AggregationBuilders.terms(GROUP_AGG)
        .field(BROWSE_PATH)
        .size(AGGREGATION_MAX_SIZE)
        .includeExclude(new IncludeExclude(currentLevel, nextLevel));
  }

  /**
   * Constructs group search request.
   *
   * @param path the path which is being browsed
   * @return {@link SearchRequest}
   */
  @Nonnull
  protected SearchRequest constructGroupsSearchRequest(
      @Nonnull String indexName, @Nonnull String path, @Nonnull Map<String, String> requestMap) {
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(buildQueryString(path, requestMap, true));
    searchSourceBuilder.aggregation(buildAggregations(path));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  /**
   * Builds query string.
   *
   * @param path the path which is being browsed
   * @param requestMap entity filters e.g. status=PUBLISHED for features
   * @param isGroupQuery true if it's group query false otherwise
   * @return {@link QueryBuilder}
   */
  @Nonnull
  private QueryBuilder buildQueryString(
      @Nonnull String path, @Nonnull Map<String, String> requestMap, boolean isGroupQuery) {
    final int browseDepthVal = getPathDepth(path);

    final BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

    queryBuilder.mustNot(QueryBuilders.termQuery(REMOVED, "true"));

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.termQuery(BROWSE_PATH, path));
    }

    if (isGroupQuery) {
      queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_DEPTH).gt(browseDepthVal));
    } else {
      queryBuilder.filter(QueryBuilders.termQuery(BROWSE_PATH_DEPTH, browseDepthVal));
    }

    requestMap.forEach((field, val) -> queryBuilder.filter(QueryBuilders.termQuery(field, val)));

    return queryBuilder;
  }

  /**
   * Constructs search request for entity search.
   *
   * @param path the path which is being browsed
   * @param from index of first entity
   * @param size count of entities
   * @return {@link SearchRequest}
   */
  @VisibleForTesting
  @Nonnull
  SearchRequest constructEntitiesSearchRequest(
      @Nonnull String indexName,
      @Nonnull String path,
      @Nonnull Map<String, String> requestMap,
      int from,
      int size) {
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);
    searchSourceBuilder.fetchSource(new String[] {BROWSE_PATH, URN}, null);
    searchSourceBuilder.sort(URN, SortOrder.ASC);
    searchSourceBuilder.query(buildQueryString(path, requestMap, false));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  /**
   * Constructs search request for entity search.
   *
   * @param path the path which is being browsed
   * @param sort the sort values of the last search result in the previous page
   * @param pitId the PointInTime ID of the previous request
   * @param keepAlive keepAlive string representation of time to keep point in time alive
   * @param size count of entities
   * @return {@link SearchRequest}
   */
  @VisibleForTesting
  @Nonnull
  SearchRequest constructEntitiesSearchRequest(
      @Nonnull String indexName,
      @Nonnull String path,
      @Nonnull Map<String, String> requestMap,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nonnull String keepAlive,
      int size) {
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);

    searchSourceBuilder.size(size);
    searchSourceBuilder.fetchSource(new String[] {BROWSE_PATH, URN}, null);
    searchSourceBuilder.sort(URN, SortOrder.ASC);
    searchSourceBuilder.query(buildQueryString(path, requestMap, false));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  /**
   * Extracts group search response into browse result metadata.
   *
   * @param groupsResponse groups search response
   * @param path the path which is being browsed
   * @return {@link BrowseResultMetadata}
   */
  @Nonnull
  private BrowseGroupsResult extractGroupsResponse(
      @Nonnull SearchResponse groupsResponse, @Nonnull String path, int from, int size) {
    final ParsedTerms groups = groupsResponse.getAggregations().get(GROUP_AGG);
    final List<BrowseResultGroup> groupsAgg =
        groups.getBuckets().stream()
            .map(
                group ->
                    new BrowseResultGroup()
                        .setName(getSimpleName(group.getKeyAsString()))
                        .setCount(group.getDocCount()))
            .collect(Collectors.toList());
    // Get the groups that are in the from to from + size range
    final List<BrowseResultGroup> paginatedGroups =
        groupsAgg.size() <= from
            ? Collections.emptyList()
            : groupsAgg.subList(from, Math.min(from + size, groupsAgg.size()));
    return new BrowseGroupsResult(
        paginatedGroups, groupsAgg.size(), (int) groupsResponse.getHits().getTotalHits().value);
  }

  /**
   * Extracts entity search response into list of browse result entities.
   *
   * @param entitiesResponse entity search response
   * @return list of {@link BrowseResultEntity}
   */
  @VisibleForTesting
  @Nonnull
  List<BrowseResultEntity> extractEntitiesResponse(
      @Nonnull SearchResponse entitiesResponse, @Nonnull String currentPath) {
    final List<BrowseResultEntity> entityMetadataArray = new ArrayList<>();
    Arrays.stream(entitiesResponse.getHits().getHits())
        .forEach(
            hit -> {
              try {
                final List<String> allPaths = (List<String>) hit.getSourceAsMap().get(BROWSE_PATH);
                entityMetadataArray.add(
                    new BrowseResultEntity()
                        .setName((String) hit.getSourceAsMap().get(URN))
                        .setUrn(Urn.createFromString((String) hit.getSourceAsMap().get(URN))));
              } catch (URISyntaxException e) {
                log.error("URN is not valid: " + e.toString());
              }
            });
    return entityMetadataArray;
  }

  /**
   * Extracts the name of group from path.
   *
   * <p>Example: /foo/bar/baz => baz
   *
   * @param path path of the group/entity
   * @return String
   */
  @Nonnull
  private String getSimpleName(@Nonnull String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  private static int getPathDepth(@Nonnull String path) {
    return StringUtils.countMatches(path, "/");
  }

  /**
   * Gets a list of paths for a given urn.
   *
   * @param entityName type of entity to query
   * @param urn urn of the entity
   * @return all paths related to a given urn
   */
  @Nonnull
  public List<String> getBrowsePaths(@Nonnull String entityName, @Nonnull Urn urn) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    final SearchRequest searchRequest = new SearchRequest(indexName);
    searchRequest.source(
        new SearchSourceBuilder().query(QueryBuilders.termQuery(URN, urn.toString())));
    final SearchHit[] searchHits;
    try {
      searchHits = client.search(searchRequest, RequestOptions.DEFAULT).getHits().getHits();
    } catch (Exception e) {
      log.error("Get paths from urn query failed: " + e.getMessage());
      throw new ESQueryException("Get paths from urn query failed: ", e);
    }

    if (searchHits.length == 0) {
      return Collections.emptyList();
    }
    final Map sourceMap = searchHits[0].getSourceAsMap();
    if (!sourceMap.containsKey(BROWSE_PATH)) {
      return Collections.emptyList();
    }
    return (List<String>) sourceMap.get(BROWSE_PATH);
  }

  public BrowseResultV2 browseV2(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count) {
    try {
      final SearchResponse groupsResponse;
      try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esGroupSearch").time()) {
        final String finalInput = input.isEmpty() ? "*" : input;
        groupsResponse =
            client.search(
                constructGroupsSearchRequestV2(entityName, path, filter, finalInput),
                RequestOptions.DEFAULT);
      }

      final BrowseGroupsResultV2 browseGroupsResult =
          extractGroupsResponseV2(groupsResponse, path, start, count);
      final int numGroups = browseGroupsResult.getTotalGroups();

      return new BrowseResultV2()
          .setMetadata(
              new BrowseResultMetadata()
                  .setTotalNumEntities(browseGroupsResult.getTotalNumEntities())
                  .setPath(path))
          .setGroups(new BrowseResultGroupV2Array(browseGroupsResult.getGroups()))
          .setNumGroups(numGroups)
          .setFrom(start)
          .setPageSize(count);
    } catch (Exception e) {
      log.error("Browse V2 query failed: " + e.getMessage());
      throw new ESQueryException("Browse V2 query failed: ", e);
    }
  }

  @Nonnull
  private SearchRequest constructGroupsSearchRequestV2(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(
        buildQueryStringV2(
            entityName,
            path,
            SearchUtil.transformFilterForEntities(filter, indexConvention),
            input));
    searchSourceBuilder.aggregation(buildAggregationsV2(path));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  /**
   * Extracts the name of group from path.
   *
   * <p>Example: ␟foo␟bar␟baz => baz
   *
   * @param path path of the group/entity
   * @return String
   */
  @Nonnull
  private String getSimpleNameV2(@Nonnull String path) {
    return path.substring(path.lastIndexOf(BROWSE_V2_DELIMITER) + 1);
  }

  private static int getPathDepthV2(@Nonnull String path) {
    return StringUtils.countMatches(path, BROWSE_V2_DELIMITER);
  }

  @Nonnull
  private QueryBuilder buildQueryStringV2(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input) {
    final int browseDepthVal = getPathDepthV2(path);

    final BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    QueryBuilder query =
        SearchRequestHandler.getBuilder(entitySpec, searchConfiguration, customSearchConfiguration)
            .getQuery(input, false);
    queryBuilder.must(query);

    filterSoftDeletedByDefault(filter, queryBuilder);

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.matchQuery(BROWSE_PATH_V2, path));
    }

    queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_V2_DEPTH).gt(browseDepthVal));

    queryBuilder.filter(SearchRequestHandler.getFilterQuery(filter));

    return queryBuilder;
  }

  @Nonnull
  private AggregationBuilder buildAggregationsV2(@Nonnull String path) {
    final String currentLevel = ESUtils.escapeReservedCharacters(path) + "␟.*";
    final String nextLevel = ESUtils.escapeReservedCharacters(path) + "␟.*␟.*";
    final String subAggNextLevel = ESUtils.escapeReservedCharacters(path) + "␟.*␟.*␟.*";

    return AggregationBuilders.terms(GROUP_AGG)
        .field(BROWSE_PATH_V2)
        .size(AGGREGATION_MAX_SIZE)
        .subAggregation(
            AggregationBuilders.terms(GROUP_AGG)
                .field(BROWSE_PATH_V2)
                .size(1) // only need to know if there are groups below, not how many
                .includeExclude(new IncludeExclude(nextLevel, subAggNextLevel)))
        .includeExclude(new IncludeExclude(currentLevel, nextLevel));
  }

  /**
   * Extracts group search response into browse result metadata.
   *
   * @param groupsResponse groups search response
   * @param path the path which is being browsed
   * @return {@link BrowseResultMetadata}
   */
  @Nonnull
  private BrowseGroupsResultV2 extractGroupsResponseV2(
      @Nonnull SearchResponse groupsResponse, @Nonnull String path, int from, int size) {
    final ParsedTerms groups = groupsResponse.getAggregations().get(GROUP_AGG);
    final List<BrowseResultGroupV2> groupsAgg =
        groups.getBuckets().stream().map(this::mapBrowseResultGroupV2).collect(Collectors.toList());

    // Get the groups that are in the from to from + size range
    final List<BrowseResultGroupV2> paginatedGroups =
        groupsAgg.size() <= from
            ? Collections.emptyList()
            : groupsAgg.subList(from, Math.min(from + size, groupsAgg.size()));
    return new BrowseGroupsResultV2(
        paginatedGroups, groupsAgg.size(), (int) groupsResponse.getHits().getTotalHits().value);
  }

  private boolean hasSubGroups(Terms.Bucket group) {
    final ParsedTerms subGroups = group.getAggregations().get(GROUP_AGG);
    if (subGroups != null) {
      return subGroups.getBuckets().size() > 0;
    }
    return false;
  }

  private BrowseResultGroupV2 mapBrowseResultGroupV2(Terms.Bucket group) {
    BrowseResultGroupV2 browseGroup = new BrowseResultGroupV2();
    String name = getSimpleNameV2(group.getKeyAsString());
    browseGroup.setName(name);
    browseGroup.setHasSubGroups(hasSubGroups(group));
    browseGroup.setCount(group.getDocCount());
    if (name.startsWith("urn:li:")) {
      browseGroup.setUrn(UrnUtils.getUrn(name));
    }
    return browseGroup;
  }
}
