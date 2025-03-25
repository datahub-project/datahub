package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.search.utils.ESUtils.applyDefaultSearchFilters;
import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

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
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;
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
@Accessors(chain = true)
public class ESBrowseDAO {

  private final RestHighLevelClient client;
  @Nonnull private final SearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;

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

  private static final SearchFlags DEFAULT_BROWSE_SEARCH_FLAGS =
      new SearchFlags()
          .setFulltext(true)
          .setSkipHighlighting(true)
          .setGetSuggestions(false)
          .setIncludeSoftDeleted(false)
          .setIncludeRestricted(false);

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
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      int size) {
    final Map<String, List<String>> requestMap = SearchUtils.getRequestMap(filters);

    final OperationContext finalOpContext =
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, path, DEFAULT_BROWSE_SEARCH_FLAGS));

    try {
      final String indexName =
          opContext
              .getSearchContext()
              .getIndexConvention()
              .getIndexName(opContext.getEntityRegistry().getEntitySpec(entityName));

      final SearchResponse groupsResponse =
          opContext.withSpan(
              "esGroupSearch",
              () -> {
                try {
                  return client.search(
                      constructGroupsSearchRequest(finalOpContext, indexName, path, requestMap),
                      RequestOptions.DEFAULT);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              },
              MetricUtils.DROPWIZARD_NAME,
              MetricUtils.name(this.getClass(), "esGroupSearch"));

      final BrowseGroupsResult browseGroupsResult =
          extractGroupsResponse(groupsResponse, path, from, size);
      final int numGroups = browseGroupsResult.getTotalGroups();

      // Based on the number of groups returned, compute the from and size to query for entities
      // Groups come before entities, so if numGroups >= from + size, we should return all groups
      // if from < numGroups < from + size, we should return a mix of groups and entities
      // if numGroups <= from, we should only return entities
      int entityFrom = Math.max(from - numGroups, 0);
      int entitySize = Math.min(Math.max(from + size - numGroups, 0), size);
      final SearchResponse entitiesResponse =
          opContext.withSpan(
              "esEntitiesSearch",
              () -> {
                try {
                  return client.search(
                      constructEntitiesSearchRequest(
                          finalOpContext, indexName, path, requestMap, entityFrom, entitySize),
                      RequestOptions.DEFAULT);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              },
              MetricUtils.DROPWIZARD_NAME,
              MetricUtils.name(this.getClass(), "esEntitiesSearch"));

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
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull String path,
      @Nonnull Map<String, List<String>> requestMap) {
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(buildQueryString(opContext, path, requestMap, true));
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
      @Nonnull OperationContext opContext,
      @Nonnull String path,
      @Nonnull Map<String, List<String>> requestMap,
      boolean isGroupQuery) {
    final int browseDepthVal = getPathDepth(path);

    final BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

    applyDefaultSearchFilters(opContext, null, queryBuilder);

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.termQuery(BROWSE_PATH, path));
    }

    if (isGroupQuery) {
      queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_DEPTH).gt(browseDepthVal));
    } else {
      queryBuilder.filter(QueryBuilders.termQuery(BROWSE_PATH_DEPTH, browseDepthVal));
    }

    requestMap.forEach((field, vals) -> queryBuilder.filter(QueryBuilders.termsQuery(field, vals)));

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
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull String path,
      @Nonnull Map<String, List<String>> requestMap,
      int from,
      int size) {
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);
    searchSourceBuilder.fetchSource(new String[] {BROWSE_PATH, URN}, null);
    searchSourceBuilder.sort(URN, SortOrder.ASC);
    searchSourceBuilder.query(buildQueryString(opContext, path, requestMap, false));
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
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull String path,
      @Nonnull Map<String, List<String>> requestMap,
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
    searchSourceBuilder.query(buildQueryString(opContext, path, requestMap, false));
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
  public List<String> getBrowsePaths(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull Urn urn) {
    final String indexName =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getIndexName(opContext.getEntityRegistry().getEntitySpec(entityName));
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
    List<String> browsePaths =
        ((List<String>) sourceMap.get(BROWSE_PATH))
            .stream().filter(Objects::nonNull).collect(Collectors.toList());
    return browsePaths;
  }

  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count) {
    try {
      final OperationContext finalOpContext =
          opContext.withSearchFlags(
              flags -> applyDefaultSearchFlags(flags, path, DEFAULT_BROWSE_SEARCH_FLAGS));

      final SearchResponse groupsResponse =
          opContext.withSpan(
              "esGroupSearch",
              () -> {
                try {
                  return client.search(
                      constructGroupsSearchRequestV2(
                          finalOpContext, entityName, path, filter, input.isEmpty() ? "*" : input),
                      RequestOptions.DEFAULT);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              },
              MetricUtils.DROPWIZARD_NAME,
              MetricUtils.name(this.getClass(), "esGroupSearch"));

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

  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count) {
    try {
      final OperationContext finalOpContext =
          opContext.withSearchFlags(
              flags -> applyDefaultSearchFlags(flags, path, DEFAULT_BROWSE_SEARCH_FLAGS));

      final SearchResponse groupsResponse =
          opContext.withSpan(
              "esGroupSearch",
              () -> {
                try {
                  return client.search(
                      constructGroupsSearchRequestBrowseAcrossEntities(
                          finalOpContext, entities, path, filter, input.isEmpty() ? "*" : input),
                      RequestOptions.DEFAULT);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              },
              MetricUtils.DROPWIZARD_NAME,
              MetricUtils.name(this.getClass(), "esGroupSearch"));

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
      log.error("Browse Across Entities query failed: " + e.getMessage());
      throw new ESQueryException("Browse Across Entities query failed: ", e);
    }
  }

  @Nonnull
  private SearchRequest constructGroupsSearchRequestV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input) {
    final String indexName =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getIndexName(opContext.getEntityRegistry().getEntitySpec(entityName));
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(
        buildQueryStringV2(
            opContext,
            entityName,
            path,
            SearchUtil.transformFilterForEntities(
                filter, opContext.getSearchContext().getIndexConvention()),
            input));
    searchSourceBuilder.aggregation(buildAggregationsV2(path));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  @Nonnull
  private SearchRequest constructGroupsSearchRequestBrowseAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input) {

    List<EntitySpec> entitySpecs =
        entities.stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .collect(Collectors.toList());

    String[] indexArray =
        entities.stream()
            .map(e -> opContext.getSearchContext().getIndexConvention().getEntityIndexName(e))
            .toArray(String[]::new);

    final SearchRequest searchRequest = new SearchRequest(indexArray);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(
        buildQueryStringBrowseAcrossEntities(
            opContext,
            entitySpecs,
            path,
            SearchUtil.transformFilterForEntities(
                filter, opContext.getSearchContext().getIndexConvention()),
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
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input) {

    final OperationContext finalOpContext =
        opContext.withSearchFlags(
            flags -> Optional.ofNullable(flags).orElse(new SearchFlags().setFulltext(true)));

    final int browseDepthVal = getPathDepthV2(path);

    final BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    QueryBuilder query =
        SearchRequestHandler.getBuilder(
                opContext,
                entitySpec,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain)
            .getQuery(
                finalOpContext,
                input,
                Boolean.TRUE.equals(
                    finalOpContext.getSearchContext().getSearchFlags().isFulltext()));
    queryBuilder.must(query);

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.matchQuery(BROWSE_PATH_V2, path));
    }

    queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_V2_DEPTH).gt(browseDepthVal));

    queryBuilder.filter(
        SearchRequestHandler.getFilterQuery(
            finalOpContext, filter, entitySpec.getSearchableFieldTypes(), queryFilterRewriteChain));

    return queryBuilder;
  }

  @Nonnull
  private QueryBuilder buildQueryStringBrowseAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input) {
    final OperationContext finalOpContext =
        opContext.withSearchFlags(
            flags -> Optional.ofNullable(flags).orElse(new SearchFlags().setFulltext(true)));
    final int browseDepthVal = getPathDepthV2(path);

    final BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

    QueryBuilder query =
        SearchRequestHandler.getBuilder(
                finalOpContext,
                entitySpecs,
                searchConfiguration,
                customSearchConfiguration,
                queryFilterRewriteChain)
            .getQuery(
                finalOpContext,
                input,
                Boolean.TRUE.equals(
                    finalOpContext.getSearchContext().getSearchFlags().isFulltext()));
    queryBuilder.must(query);

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.matchQuery(BROWSE_PATH_V2, path));
    }

    queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_V2_DEPTH).gt(browseDepthVal));

    Map<String, Set<SearchableAnnotation.FieldType>> searchableFields =
        entitySpecs.stream()
            .flatMap(entitySpec -> entitySpec.getSearchableFieldTypes().entrySet().stream())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (set1, set2) -> {
                      set1.addAll(set2);
                      return set1;
                    }));
    queryBuilder.filter(
        SearchRequestHandler.getFilterQuery(
            finalOpContext, filter, searchableFields, queryFilterRewriteChain));

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
