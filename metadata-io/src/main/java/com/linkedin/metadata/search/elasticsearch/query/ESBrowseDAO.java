package com.linkedin.metadata.search.elasticsearch.query;

import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.browse.BrowseResultGroupV2;
import com.linkedin.metadata.browse.BrowseResultGroupV2Array;
import com.linkedin.metadata.browse.BrowseResultMetadata;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchOperation;
import com.linkedin.metadata.search.utils.SearchTookMetrics;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
@RequiredArgsConstructor
@Accessors(chain = true)
public class ESBrowseDAO {

  @Nonnull private final ElasticSearchConfiguration searchConfiguration;
  @Nullable private final CustomSearchConfiguration customSearchConfiguration;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;

  @Nonnull
  private SearchClientShim<?> searchClient(
      @Nonnull OperationContext opContext, @Nullable String... indices) {
    return SearchClients.forEntityIndices(opContext, searchConfiguration, indices);
  }

  private static final String BROWSE_PATH_V2 = "browsePathV2";
  private static final String BROWSE_PATH_V2_DEPTH = "browsePathV2.length";
  private static final String BROWSE_V2_DELIMITER = "␟";

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
  private class BrowseGroupsResultV2 {
    List<BrowseResultGroupV2> groups;
    int totalGroups;
    int totalNumEntities;
  }

  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      @Nullable Integer count) {
    try {
      final OperationContext finalOpContext =
          opContext.withSearchFlags(
              flags -> applyDefaultSearchFlags(flags, path, DEFAULT_BROWSE_SEARCH_FLAGS));
      count = ConfigUtils.applyLimit(searchServiceConfig, count);
      final String indexName = entityIndexName(finalOpContext, entityName);

      final SearchResponse groupsResponse =
          opContext.withSpan(
              "esGroupSearch",
              () -> {
                try {
                  return SearchTookMetrics.time(
                      finalOpContext,
                      SearchOperation.BROWSE,
                      () ->
                          searchClient(finalOpContext, indexName)
                              .search(
                                  finalOpContext,
                                  constructGroupsSearchRequestV2(
                                      finalOpContext,
                                      entityName,
                                      path,
                                      filter,
                                      input.isEmpty() ? "*" : input),
                                  RequestOptions.DEFAULT));
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
      @Nullable Integer count) {
    try {
      final OperationContext finalOpContext =
          opContext.withSearchFlags(
              flags -> applyDefaultSearchFlags(flags, path, DEFAULT_BROWSE_SEARCH_FLAGS));
      count = ConfigUtils.applyLimit(searchServiceConfig, count);
      final String[] indexNames = entityIndexNames(finalOpContext, entities);

      final SearchResponse groupsResponse =
          opContext.withSpan(
              "esGroupSearch",
              () -> {
                try {
                  return SearchTookMetrics.time(
                      finalOpContext,
                      SearchOperation.BROWSE,
                      () ->
                          searchClient(finalOpContext, indexNames)
                              .search(
                                  finalOpContext,
                                  constructGroupsSearchRequestBrowseAcrossEntities(
                                      finalOpContext,
                                      entities,
                                      path,
                                      filter,
                                      input.isEmpty() ? "*" : input),
                                  RequestOptions.DEFAULT));
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
    final String indexName = entityIndexName(opContext, entityName);
    final SearchRequest searchRequest = new SearchRequest(indexName);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    // The browse template owns lexical routing and ranking. Path, depth, authorization filters,
    // the groups aggregation, the result limit, and execution stay here, so a stored aggregation
    // or rescore cannot replace group counts.
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(
        buildQueryStringV2(
            opContext,
            entityName,
            path,
            SearchUtil.transformFilterForEntities(
                opContext,
                filter,
                opContext.getSearchContext().getIndexConvention(),
                rewriteEntityTypeToIndex()),
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

    String[] indexArray = entityIndexNames(opContext, entities);

    final SearchRequest searchRequest = new SearchRequest(indexArray);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    // Same boundary as a single-entity browse: the template query, then Java path, depth,
    // filters, and the groups aggregation.
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(
        buildQueryStringBrowseAcrossEntities(
            opContext,
            entitySpecs,
            path,
            SearchUtil.transformFilterForEntities(
                opContext,
                filter,
                opContext.getSearchContext().getIndexConvention(),
                rewriteEntityTypeToIndex()),
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
    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            opContext,
            entitySpec,
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            searchServiceConfig);
    QueryBuilder query =
        handler.getQuery(
            finalOpContext,
            input,
            Boolean.TRUE.equals(finalOpContext.getSearchContext().getSearchFlags().isFulltext()));
    queryBuilder.must(query);

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.matchQuery(BROWSE_PATH_V2, path));
    }

    queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_V2_DEPTH).gt(browseDepthVal));

    queryBuilder.filter(handler.getFilterQuery(finalOpContext, filter));

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

    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            finalOpContext,
            entitySpecs,
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            searchServiceConfig);
    QueryBuilder query =
        handler.getQuery(
            finalOpContext,
            input,
            Boolean.TRUE.equals(finalOpContext.getSearchContext().getSearchFlags().isFulltext()));
    queryBuilder.must(query);

    if (!path.isEmpty()) {
      queryBuilder.filter(QueryBuilders.matchQuery(BROWSE_PATH_V2, path));
    }

    queryBuilder.filter(QueryBuilders.rangeQuery(BROWSE_PATH_V2_DEPTH).gt(browseDepthVal));

    queryBuilder.filter(handler.getFilterQuery(finalOpContext, filter));

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
      @Nonnull SearchResponse groupsResponse,
      @Nonnull String path,
      int from,
      @Nullable Integer size) {
    size = ConfigUtils.applyLimit(searchServiceConfig, size);
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

  private boolean rewriteEntityTypeToIndex() {
    return !EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex());
  }

  @Nonnull
  private String[] entityIndexNames(
      @Nonnull OperationContext opContext, @Nonnull List<String> entityNames) {
    return EntitySearchIndexResolver.indexNames(
        opContext, entityNames, searchConfiguration.getEntityIndex());
  }

  @Nonnull
  private String entityIndexName(@Nonnull OperationContext opContext, @Nonnull String entityName) {
    return EntitySearchIndexResolver.indexName(
        opContext, entityName, searchConfiguration.getEntityIndex());
  }
}
