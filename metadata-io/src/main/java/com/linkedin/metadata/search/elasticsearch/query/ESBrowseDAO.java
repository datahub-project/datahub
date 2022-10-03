package com.linkedin.metadata.search.elasticsearch.query;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.browse.BrowseResultGroup;
import com.linkedin.metadata.browse.BrowseResultGroupArray;
import com.linkedin.metadata.browse.BrowseResultMetadata;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;


@Slf4j
@RequiredArgsConstructor
public class ESBrowseDAO {

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;

  private static final String BROWSE_PATH = "browsePaths";
  private static final String BROWSE_PATH_DEPTH = "browsePaths.length";
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
  public BrowseResult browse(@Nonnull String entityName, @Nonnull String path, @Nullable Filter filters, int from,
      int size) {
    final Map<String, String> requestMap = SearchUtils.getRequestMap(filters);

    try {
      final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));

      final SearchResponse groupsResponse;
      try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esGroupSearch").time()) {
        groupsResponse =
            client.search(constructGroupsSearchRequest(indexName, path, requestMap), RequestOptions.DEFAULT);
      }
      final BrowseGroupsResult browseGroupsResult = extractGroupsResponse(groupsResponse, path, from, size);
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
            client.search(constructEntitiesSearchRequest(indexName, path, requestMap, entityFrom, entitySize),
                RequestOptions.DEFAULT);
      }
      final int numEntities = (int) entitiesResponse.getHits().getTotalHits().value;
      final List<BrowseResultEntity> browseResultEntityList = extractEntitiesResponse(entitiesResponse, path);

      return new BrowseResult().setMetadata(
          new BrowseResultMetadata().setTotalNumEntities(browseGroupsResult.getTotalNumEntities()).setPath(path))
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
  protected SearchRequest constructGroupsSearchRequest(@Nonnull String indexName, @Nonnull String path,
      @Nonnull Map<String, String> requestMap) {
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
  private QueryBuilder buildQueryString(@Nonnull String path, @Nonnull Map<String, String> requestMap,
      boolean isGroupQuery) {
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
  SearchRequest constructEntitiesSearchRequest(@Nonnull String indexName, @Nonnull String path,
      @Nonnull Map<String, String> requestMap, int from, int size) {
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);
    searchSourceBuilder.fetchSource(new String[]{BROWSE_PATH, URN}, null);
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
  private BrowseGroupsResult extractGroupsResponse(@Nonnull SearchResponse groupsResponse, @Nonnull String path,
      int from, int size) {
    final ParsedTerms groups = groupsResponse.getAggregations().get(GROUP_AGG);
    final List<BrowseResultGroup> groupsAgg = groups.getBuckets()
        .stream()
        .map(group -> new BrowseResultGroup().setName(getSimpleName(group.getKeyAsString()))
            .setCount(group.getDocCount()))
        .collect(Collectors.toList());
    // Get the groups that are in the from to from + size range
    final List<BrowseResultGroup> paginatedGroups = groupsAgg.size() <= from ? Collections.emptyList()
        : groupsAgg.subList(from, Math.min(from + size, groupsAgg.size()));
    return new BrowseGroupsResult(paginatedGroups, groupsAgg.size(),
        (int) groupsResponse.getHits().getTotalHits().value);
  }

  /**
   * Extracts entity search response into list of browse result entities.
   *
   * @param entitiesResponse entity search response
   * @return list of {@link BrowseResultEntity}
   */
  @VisibleForTesting
  @Nonnull
  List<BrowseResultEntity> extractEntitiesResponse(@Nonnull SearchResponse entitiesResponse,
      @Nonnull String currentPath) {
    final List<BrowseResultEntity> entityMetadataArray = new ArrayList<>();
    Arrays.stream(entitiesResponse.getHits().getHits()).forEach(hit -> {
      try {
        final List<String> allPaths = (List<String>) hit.getSourceAsMap().get(BROWSE_PATH);
         entityMetadataArray.add(new BrowseResultEntity().setName((String) hit.getSourceAsMap().get(URN))
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
    searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.termQuery(URN, urn.toString())));
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
}
