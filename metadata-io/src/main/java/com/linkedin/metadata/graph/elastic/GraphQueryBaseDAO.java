package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_CREATED_ON;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_UPDATED_ON;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_VIA;
import static com.linkedin.metadata.aspect.models.graph.Edge.KEY_SORTS;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;
import static com.linkedin.metadata.graph.elastic.utils.GraphQueryUtils.buildQuery;
import static com.linkedin.metadata.search.utils.ESUtils.queryOptimize;

import com.datahub.util.exception.ESQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.elastic.utils.GraphFilterUtils;
import com.linkedin.metadata.graph.elastic.utils.GraphQueryConstants;
import com.linkedin.metadata.graph.elastic.utils.GraphQueryUtils;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.UrnExtractionUtils;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

/**
 * Abstract base class for graph query DAOs that provides common functionality and implements the
 * GraphQueryDAO interface.
 */
@Slf4j
public abstract class GraphQueryBaseDAO implements GraphQueryDAO {

  protected final GraphServiceConfiguration graphServiceConfig;
  protected final ElasticSearchConfiguration config;
  protected final MetricUtils metricUtils;

  public GraphQueryBaseDAO(
      GraphServiceConfiguration graphServiceConfig,
      ElasticSearchConfiguration config,
      MetricUtils metricUtils) {
    this.graphServiceConfig = graphServiceConfig;
    this.config = config;
    this.metricUtils = metricUtils;
  }

  protected abstract SearchClientShim<?> getClient();

  protected abstract List<LineageRelationship> searchWithSlices(
      @Nonnull OperationContext opContext,
      @Nonnull QueryBuilder query,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      int maxRelations, // This is the REMAINING capacity, not the original total limit
      int defaultPageSize,
      int slices,
      long remainingTime,
      Set<Urn> entityUrns,
      boolean allowPartialResults);

  private SearchResponse executeLineageSearchQuery(
      @Nonnull OperationContext opContext,
      @Nonnull final QueryBuilder query,
      final int offset,
      @Nullable Integer count) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = sharedSourceBuilder(query, offset, count);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(
        opContext.getSearchContext().getIndexConvention().getIndexName(INDEX_NAME));

    return opContext.withSpan(
        "esQuery",
        () -> {
          try {
            if (metricUtils != null)
              metricUtils.increment(
                  this.getClass(), GraphQueryConstants.SEARCH_EXECUTIONS_METRIC, 1);
            return getClient().search(searchRequest, RequestOptions.DEFAULT);
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "esQuery"));
  }

  private SearchSourceBuilder sharedSourceBuilder(
      @Nonnull final QueryBuilder query, final int offset, @Nullable Integer count) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(ConfigUtils.applyLimit(graphServiceConfig, count));

    searchSourceBuilder.query(query);
    if (config.getSearch().getGraph().isBoostViaNodes()) {
      addViaNodeBoostQuery(searchSourceBuilder);
    }
    return searchSourceBuilder;
  }

  public SearchResponse getSearchResponse(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      final int offset,
      @Nullable Integer count) {
    BoolQueryBuilder finalQuery =
        buildQuery(opContext, config.getSearch().getGraph(), graphFilters);

    return executeLineageSearchQuery(opContext, finalQuery, offset, count);
  }

  @WithSpan
  public LineageResponse getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    Map<Urn, LineageRelationship> result = new HashMap<>();
    count = ConfigUtils.applyLimit(graphServiceConfig, count);
    long currentTime = System.currentTimeMillis();
    long remainingTime = config.getSearch().getGraph().getTimeoutSeconds() * 1000;
    boolean exploreMultiplePaths = config.getSearch().getGraph().isEnableMultiPathSearch();
    long timeoutTime = currentTime + remainingTime;

    // Do a Level-order BFS
    Set<Urn> visitedEntities = ConcurrentHashMap.newKeySet();
    visitedEntities.add(entityUrn);
    Set<Urn> viaEntities = ConcurrentHashMap.newKeySet();
    ThreadSafePathStore existingPaths = new ThreadSafePathStore();
    List<Urn> currentLevel = ImmutableList.of(entityUrn);

    for (int i = 0; i < maxHops; i++) {
      if (currentLevel.isEmpty()) {
        break;
      }

      if (remainingTime < 0) {
        log.info(
            "Timed out while fetching lineage for {} with direction {}, maxHops {}. Returning results so far",
            entityUrn,
            lineageGraphFilters.getLineageDirection(),
            maxHops);
        break;
      }

      // Do one hop on the lineage graph
      Stream<Urn> intermediateStream =
          processOneHopLineage(
              opContext,
              currentLevel,
              remainingTime,
              maxHops,
              lineageGraphFilters,
              visitedEntities,
              viaEntities,
              existingPaths,
              exploreMultiplePaths,
              result,
              i);
      currentLevel = intermediateStream.collect(Collectors.toList());
      currentTime = System.currentTimeMillis();
      remainingTime = timeoutTime - currentTime;
    }
    List<LineageRelationship> resultList = new ArrayList<>(result.values());
    LineageResponse response = new LineageResponse(resultList.size(), resultList, false);

    List<LineageRelationship> subList;
    if (offset >= response.getTotal()) {
      subList = Collections.emptyList();
    } else {
      subList =
          response
              .getLineageRelationships()
              .subList(offset, Math.min(offset + count, response.getTotal()));
    }

    return new LineageResponse(response.getTotal(), subList, response.isPartial());
  }

  private Stream<Urn> processOneHopLineage(
      @Nonnull OperationContext opContext,
      List<Urn> currentLevel,
      Long remainingTime,
      int maxHops,
      LineageGraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      ThreadSafePathStore existingPaths,
      boolean exploreMultiplePaths,
      Map<Urn, LineageRelationship> result,
      int i) {

    final LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();

    // Do one hop on the lineage graph
    int numHops = i + 1; // Zero indexed for loop counter, one indexed count
    int remainingHops = maxHops - numHops;
    List<LineageRelationship> oneHopRelationships =
        getLineageRelationshipsInBatches(
            opContext,
            currentLevel,
            graphFilters,
            visitedEntities,
            viaEntities,
            numHops,
            remainingHops,
            remainingTime,
            existingPaths,
            exploreMultiplePaths);

    for (LineageRelationship oneHopRelnship : oneHopRelationships) {
      if (result.containsKey(oneHopRelnship.getEntity())) {
        log.debug("Urn encountered again during graph walk {}", oneHopRelnship.getEntity());
        result.put(
            oneHopRelnship.getEntity(),
            mergeLineageRelationships(result.get(oneHopRelnship.getEntity()), oneHopRelnship));
      } else {
        result.put(oneHopRelnship.getEntity(), oneHopRelnship);
      }
    }
    Stream<Urn> intermediateStream =
        oneHopRelationships.stream().map(LineageRelationship::getEntity);

    if (lineageFlags != null) {
      // Recursively increase the size of the list and append
      if (lineageFlags.getIgnoreAsHops() != null) {
        List<Urn> additionalCurrentLevel = new ArrayList<>();
        UrnArrayMap ignoreAsHops = lineageFlags.getIgnoreAsHops();
        oneHopRelationships.stream()
            .filter(
                lineageRelationship ->
                    lineageFlags.getIgnoreAsHops().keySet().stream()
                        .anyMatch(
                            entityType ->
                                entityType.equals(lineageRelationship.getEntity().getEntityType())
                                    && (CollectionUtils.isEmpty(ignoreAsHops.get(entityType))
                                        || GraphQueryUtils.platformMatches(
                                            lineageRelationship.getEntity(),
                                            ignoreAsHops.get(entityType)))))
            .forEach(
                lineageRelationship -> {
                  additionalCurrentLevel.add(lineageRelationship.getEntity());
                  lineageRelationship.setIgnoredAsHop(true);
                });
        if (!additionalCurrentLevel.isEmpty()) {
          Stream<Urn> ignoreAsHopUrns =
              processOneHopLineage(
                  opContext,
                  additionalCurrentLevel,
                  remainingTime,
                  maxHops,
                  graphFilters,
                  visitedEntities,
                  viaEntities,
                  existingPaths,
                  exploreMultiplePaths,
                  result,
                  i);
          intermediateStream = Stream.concat(intermediateStream, ignoreAsHopUrns);
        }
      }

      if (remainingHops > 0) {
        // If there are hops remaining, we expect to explore everything getting passed back to the
        // loop, barring a timeout
        List<Urn> entitiesToExplore = intermediateStream.collect(Collectors.toList());
        entitiesToExplore.forEach(urn -> result.get(urn).setExplored(true));
        // reassign the stream after consuming it
        intermediateStream = entitiesToExplore.stream();
      }
    }
    return intermediateStream;
  }

  /**
   * Merges two lineage relationship objects. The merged relationship object will have the minimum
   * degree of the two relationships, and the union of the paths. In addition, the merged
   * relationship object will have the union of the degrees in the new degrees field.
   *
   * @param existingRelationship
   * @param newRelationship
   * @return the merged relationship object
   */
  protected LineageRelationship mergeLineageRelationships(
      final LineageRelationship existingRelationship, final LineageRelationship newRelationship) {
    try {
      LineageRelationship copyRelationship = existingRelationship.copy();
      copyRelationship.setDegree(
          Math.min(existingRelationship.getDegree(), newRelationship.getDegree()));
      Set<Integer> degrees = new HashSet<>();
      if (copyRelationship.hasDegrees()) {
        degrees = copyRelationship.getDegrees().stream().collect(Collectors.toSet());
      }
      degrees.add(newRelationship.getDegree());
      copyRelationship.setDegrees(new IntegerArray(degrees));
      // Deduplicate paths when merging relationships
      Set<UrnArray> uniquePaths = new HashSet<>();
      if (existingRelationship.hasPaths()) {
        for (UrnArray path : existingRelationship.getPaths()) {
          uniquePaths.add(path);
        }
      }
      if (newRelationship.hasPaths()) {
        for (UrnArray path : newRelationship.getPaths()) {
          uniquePaths.add(path);
        }
      }

      UrnArrayArray copyPaths = new UrnArrayArray(uniquePaths.size());
      copyPaths.addAll(uniquePaths);
      copyRelationship.setPaths(copyPaths);
      return copyRelationship;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Failed to clone lineage relationship", e);
    }
  }

  // Get 1-hop lineage relationships asynchronously in batches with timeout
  @WithSpan
  public List<LineageRelationship> getLineageRelationshipsInBatches(
      @Nonnull final OperationContext opContext,
      @Nonnull List<Urn> entityUrns,
      LineageGraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      long remainingTime,
      ThreadSafePathStore existingPaths,
      boolean exploreMultiplePaths) {
    List<List<Urn>> batches =
        Lists.partition(entityUrns, config.getSearch().getGraph().getBatchSize());
    return ConcurrencyUtils.getAllCompleted(
            batches.stream()
                .map(
                    batchUrns ->
                        CompletableFuture.supplyAsync(
                            () ->
                                getLineageRelationships(
                                    opContext,
                                    batchUrns,
                                    graphFilters,
                                    visitedEntities,
                                    viaEntities,
                                    numHops,
                                    remainingHops,
                                    existingPaths,
                                    exploreMultiplePaths)))
                .collect(Collectors.toList()),
            remainingTime,
            TimeUnit.MILLISECONDS)
        .stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  // Get 1-hop lineage relationships
  @WithSpan
  private List<LineageRelationship> getLineageRelationships(
      @Nonnull final OperationContext opContext,
      @Nonnull List<Urn> entityUrns,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      boolean exploreMultiplePaths) {
    final LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();

    Map<String, Set<Urn>> urnsPerEntityType =
        entityUrns.stream().collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));
    Set<Urn> entityUrnSet = new HashSet<>(entityUrns);

    QueryBuilder finalQuery = getLineageQuery(opContext, urnsPerEntityType, lineageGraphFilters);

    SearchResponse response;
    if (lineageFlags != null && lineageFlags.getEntitiesExploredPerHopLimit() != null) {
      // Visualization path

      return relationshipsGroupQuery(
          opContext,
          entityUrnSet,
          finalQuery,
          lineageGraphFilters,
          visitedEntities,
          viaEntities,
          numHops,
          existingPaths,
          exploreMultiplePaths,
          lineageFlags.getEntitiesExploredPerHopLimit());
    } else {
      response =
          executeLineageSearchQuery(
              opContext, finalQuery, 0, graphServiceConfig.getLimit().getResults().getApiDefault());
      return GraphQueryUtils.extractRelationships(
          entityUrnSet,
          response,
          lineageGraphFilters,
          visitedEntities,
          viaEntities,
          numHops,
          remainingHops,
          existingPaths,
          exploreMultiplePaths);
    }
  }

  /**
   * Executes a search request against the graph index. This method is exposed for use by other
   * graph service methods that need direct search access.
   *
   * @param searchRequest The search request to execute
   * @return The search response from Elasticsearch
   * @throws ESQueryException if the search fails
   */
  SearchResponse executeSearch(@Nonnull SearchRequest searchRequest) {
    try {
      if (metricUtils != null)
        metricUtils.increment(this.getClass(), GraphQueryConstants.SEARCH_EXECUTIONS_METRIC, 1);
      return getClient().search(searchRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @VisibleForTesting
  public QueryBuilder getLineageQuery(
      @Nonnull OperationContext opContext,
      @Nonnull Map<String, Set<Urn>> urnsPerEntityType,
      @Nonnull LineageGraphFilters lineageGraphFilters) {
    final LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();

    BoolQueryBuilder entityTypeQueries = QueryBuilders.boolQuery();

    // Get all relation types relevant to the set of urns to hop from
    urnsPerEntityType.forEach(
        (entityType, urns) -> {
          buildLineageGraphFiltersQuery(opContext, entityType, urns, lineageGraphFilters)
              .ifPresent(entityTypeQueries::should);
        });
    entityTypeQueries.minimumShouldMatch(1);

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    finalQuery.filter(entityTypeQueries);

    /*
     * Optional - Add edge filtering based on time windows.
     */
    if (lineageFlags != null
        && lineageFlags.getStartTimeMillis() != null
        && lineageFlags.getEndTimeMillis() != null) {
      finalQuery.filter(
          GraphFilterUtils.getEdgeTimeFilterQuery(
              lineageFlags.getStartTimeMillis(), lineageFlags.getEndTimeMillis()));
    } else {
      log.debug("Empty time filter range provided. Skipping application of time filters");
    }

    return finalQuery;
  }

  /**
   * Replaces score from initial lineage query against the graph index with score from whether a via
   * edge exists or not. We don't currently sort the results for the graph query for anything else,
   * we just do a straight filter, but this will need to be re-evaluated if we do.
   *
   * @param sourceBuilder source builder for the lineage query
   */
  private void addViaNodeBoostQuery(final SearchSourceBuilder sourceBuilder) {
    QueryBuilders.functionScoreQuery(QueryBuilders.existsQuery(EDGE_FIELD_VIA))
        .boostMode(CombineFunction.REPLACE);
    QueryRescorerBuilder queryRescorerBuilder =
        new QueryRescorerBuilder(
            QueryBuilders.functionScoreQuery(QueryBuilders.existsQuery(EDGE_FIELD_VIA))
                .boostMode(CombineFunction.REPLACE));
    queryRescorerBuilder.windowSize(
        graphServiceConfig.getLimit().getResults().getApiDefault()); // Will rescore all results
    sourceBuilder.addRescorer(queryRescorerBuilder);
  }

  /**
   * Extracts lineage relationships for a batch of entities.
   *
   * <p>This method performs the lineage query for a set of entity URNs and extracts all
   * relationships matching the specified filters. It uses the streaming pagination approach to
   * efficiently process large result sets.
   *
   * @param opContext The operation context, containing request metadata and search configuration.
   * @param entityUrns The set of entity URNs representing the current level in the lineage
   *     traversal. These entities serve as the source points for finding outgoing or incoming
   *     edges.
   * @param baseQuery The Elasticsearch query that defines the base query conditions, including any
   *     filtering by entity type and relationship type.
   * @param lineageGraphFilters Filters specifying which edge types and directions are valid for the
   *     lineage traversal (incoming/outgoing edges, valid relationship types).
   * @param visitedEntities Set of entities that have already been processed in the lineage
   *     traversal, used to prevent cycles and duplicate processing.
   * @param viaEntities Set of "via" entities that have already been processed. "Via" entities are
   *     intermediate entities that connect other entities in a lineage path.
   * @param numHops The current number of hops from the origin entity in the lineage traversal, used
   *     to track the traversal depth.
   * @param existingPaths Map of entity URN to arrays of paths leading to that entity. Each path is
   *     an ordered array of URNs representing the traversal from the origin to the entity.
   * @param exploreMultiplePaths If true, multiple paths to the same entity will be explored. If
   *     false, only the first discovered path to an entity is explored.
   * @return A list of LineageRelationship objects representing the relationships extracted from the
   *     query. Each relationship includes the entity URN, degree (hop count), relationship type,
   *     and paths to reach the entity.
   */
  @WithSpan
  private List<LineageRelationship> relationshipsGroupQuery(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull QueryBuilder baseQuery,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      ThreadSafePathStore existingPaths,
      boolean exploreMultiplePaths,
      @Nullable final Integer entitiesPerHopLimit) {

    Set<Urn> entityUrnSet = new HashSet<>(entityUrns);

    if (config.getSearch().getGraph().isQueryOptimization()) {
      queryOptimize(baseQuery, false);
    }

    // Get search responses as a stream with pagination
    Stream<SearchResponse> responseStream =
        executeGroupByLineageSearchQuery(
            opContext,
            baseQuery,
            lineageGraphFilters,
            graphServiceConfig.getLimit().getResults().getApiDefault(),
            exploreMultiplePaths,
            entitiesPerHopLimit,
            entityUrns);

    // Process the stream of responses to extract relationships
    return GraphQueryUtils.extractRelationshipsFromSearchResponses(
        entityUrnSet,
        responseStream,
        lineageGraphFilters,
        visitedEntities,
        viaEntities,
        numHops,
        existingPaths,
        exploreMultiplePaths,
        entitiesPerHopLimit);
  }

  public SearchResponse getSearchResponse(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count) {

    BoolQueryBuilder finalQuery =
        buildQuery(opContext, config.getSearch().getGraph(), graphFilters);

    return executeScrollSearchQuery(
        opContext, finalQuery, sortCriteria, scrollId, keepAlive, count);
  }

  private SearchResponse executeScrollSearchQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final QueryBuilder query,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count) {

    boolean hasSliceOptions = opContext.getSearchContext().getSearchFlags().hasSliceOptions();
    boolean usePIT =
        (config.getSearch().getGraph().isPointInTimeCreationEnabled() || hasSliceOptions)
            && keepAlive != null;
    String pitId =
        usePIT
            ? ESUtils.computePointInTime(
                scrollId,
                keepAlive,
                getClient(),
                opContext.getSearchContext().getIndexConvention().getIndexName(INDEX_NAME))
            : null;
    Object[] sort = scrollId != null ? SearchAfterWrapper.fromScrollId(scrollId).getSort() : null;

    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.size(ConfigUtils.applyLimit(graphServiceConfig, count));
    searchSourceBuilder.query(query);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, List.of(), false);
    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);
    ESUtils.setSliceOptions(
        searchSourceBuilder, opContext.getSearchContext().getSearchFlags().getSliceOptions());
    searchRequest.source(searchSourceBuilder);

    // PIT specifies indices in creation so it doesn't support specifying indices on the request
    if (!usePIT) {
      searchRequest.indices(
          opContext.getSearchContext().getIndexConvention().getIndexName(INDEX_NAME));
    }

    return opContext.withSpan(
        "esQuery",
        () -> {
          try {
            if (metricUtils != null)
              metricUtils.increment(
                  this.getClass(), GraphQueryConstants.SEARCH_EXECUTIONS_METRIC, 1);
            return getClient().search(searchRequest, RequestOptions.DEFAULT);
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "esQuery"));
  }

  /**
   * @param lineageGraphFilters lineage graph filters being applied
   * @return elasticsearch boolean filter query
   */
  private Optional<BoolQueryBuilder> buildLineageGraphFiltersQuery(
      OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Set<Urn> urns,
      @Nonnull LineageGraphFilters lineageGraphFilters) {

    if (urns.stream().anyMatch(urn -> !entityType.equals(urn.getEntityType()))) {
      throw new IllegalArgumentException("Urns must be of the same entity type.");
    }

    Set<LineageRegistry.EdgeInfo> edgeInfo =
        lineageGraphFilters.getEdgeInfo(opContext.getLineageRegistry(), entityType);

    if (urns.isEmpty() || edgeInfo.isEmpty()) {
      return Optional.empty();
    } else {
      // Create the main bool query
      BoolQueryBuilder mainQuery = QueryBuilders.boolQuery();
      Set<String> entityUrns = urns.stream().map(Urn::toString).collect(Collectors.toSet());

      // Group edge info by relationship type AND direction
      Map<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>> edgeGroups =
          edgeInfo.stream()
              .collect(Collectors.groupingBy(edge -> Pair.of(edge.getType(), edge.getDirection())));

      // Special handling for UNDIRECTED - they need to be included in both directions
      List<LineageRegistry.EdgeInfo> undirectedEdges =
          edgeInfo.stream()
              .filter(edge -> edge.getDirection() == RelationshipDirection.UNDIRECTED)
              .collect(Collectors.toList());

      // Add undirected edges to both INCOMING and OUTGOING groups
      for (LineageRegistry.EdgeInfo undirectedEdge : undirectedEdges) {
        // Create virtual INCOMING edge
        edgeGroups
            .computeIfAbsent(
                Pair.of(undirectedEdge.getType(), RelationshipDirection.INCOMING),
                k -> new ArrayList<>())
            .add(undirectedEdge);

        // Create virtual OUTGOING edge
        edgeGroups
            .computeIfAbsent(
                Pair.of(undirectedEdge.getType(), RelationshipDirection.OUTGOING),
                k -> new ArrayList<>())
            .add(undirectedEdge);
      }

      // Process each group
      for (Map.Entry<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>> entry :
          edgeGroups.entrySet()) {
        String relationshipType = entry.getKey().getLeft();
        RelationshipDirection direction = entry.getKey().getRight();
        List<LineageRegistry.EdgeInfo> edges = entry.getValue();

        // Skip the UNDIRECTED in the main loop as we've already processed them
        if (direction == RelationshipDirection.UNDIRECTED) {
          continue;
        }

        // Collect the entity types for this relationship type and direction
        List<String> entityTypes =
            edges.stream()
                .map(LineageRegistry.EdgeInfo::getOpposingEntityType)
                .collect(Collectors.toList());

        // Build the appropriate query based on direction
        if (direction == RelationshipDirection.OUTGOING) {
          BoolQueryBuilder outgoingQuery =
              QueryBuilders.boolQuery()
                  .filter(QueryBuilders.termsQuery(GraphQueryConstants.SOURCE_URN, entityUrns))
                  .filter(
                      QueryBuilders.termQuery(
                          GraphQueryConstants.RELATIONSHIP_TYPE, relationshipType));

          // Use termsQuery for multiple types
          outgoingQuery.filter(
              QueryBuilders.termsQuery(GraphQueryConstants.DESTINATION_TYPE, entityTypes));

          mainQuery.should(outgoingQuery);
        } else if (direction == RelationshipDirection.INCOMING) {
          BoolQueryBuilder incomingQuery =
              QueryBuilders.boolQuery()
                  .filter(QueryBuilders.termsQuery(GraphQueryConstants.DESTINATION_URN, entityUrns))
                  .filter(
                      QueryBuilders.termQuery(
                          GraphQueryConstants.RELATIONSHIP_TYPE, relationshipType));

          // Use termsQuery for multiple types
          incomingQuery.filter(
              QueryBuilders.termsQuery(GraphQueryConstants.SOURCE_TYPE, entityTypes));

          mainQuery.should(incomingQuery);
        }
      }

      // Require that at least one of the "should" clauses matches
      mainQuery.minimumShouldMatch(1);

      return Optional.of(mainQuery);
    }
  }

  /**
   * Executes lineage graph search queries in parallel for both incoming and outgoing relationships.
   *
   * <p>This method creates and executes Elasticsearch queries to find lineage relationships,
   * supporting both incoming and outgoing edges in parallel. It uses search_after pagination to
   * efficiently process large result sets, returning a combined stream of search responses.
   *
   * @param opContext The operation context, containing request metadata and tracing information.
   *     Used for creating spans and accessing search configuration.
   * @param query The base Elasticsearch query to which direction-specific filters will be added.
   *     This typically includes entity type and relationship type filters.
   * @param lineageGraphFilters Filters that define valid edge types and directions for lineage
   *     traversal. Used to determine which relationships to include in the results.
   * @param pageSize The number of documents to retrieve in each pagination request. Controls the
   *     batch size for each Elasticsearch query.
   * @param entitiesPerHopLimit Optional limit on the total number of entities to explore per hop.
   * @return A stream of SearchResponse objects containing the lineage relationships. The stream
   *     includes results from both incoming and outgoing edges, paginated using the search_after
   *     mechanism.
   * @throws ESQueryException If there is an error executing the Elasticsearch queries or if the
   *     queries time out based on the configured timeout.
   */
  @WithSpan
  private Stream<SearchResponse> executeGroupByLineageSearchQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final QueryBuilder query,
      final LineageGraphFilters lineageGraphFilters,
      final int pageSize,
      boolean exploreMultiplePaths,
      @Nullable final Integer entitiesPerHopLimit,
      Set<Urn> originalEntityUrns) {

    if (entitiesPerHopLimit != null && entitiesPerHopLimit == 0) {
      log.warn("Requested 0 entities to explore per hop?");
      return Stream.empty();
    }

    // Create source filter for outgoing relationships
    BoolQueryBuilder sourceFilterQuery = QueryBuilders.boolQuery();
    lineageGraphFilters
        .streamEdgeInfo()
        .filter(pair -> RelationshipDirection.OUTGOING.equals(pair.getValue().getDirection()))
        .forEach(
            pair ->
                sourceFilterQuery.should(
                    GraphFilterUtils.getAggregationFilter(pair, RelationshipDirection.OUTGOING)));

    if (!sourceFilterQuery.should().isEmpty()) {
      sourceFilterQuery.minimumShouldMatch(1);
    }

    // Create destination filter for incoming relationships
    BoolQueryBuilder destFilterQuery = QueryBuilders.boolQuery();
    lineageGraphFilters
        .streamEdgeInfo()
        .filter(pair -> RelationshipDirection.INCOMING.equals(pair.getValue().getDirection()))
        .forEach(
            pair ->
                destFilterQuery.should(
                    GraphFilterUtils.getAggregationFilter(pair, RelationshipDirection.INCOMING)));

    if (!destFilterQuery.should().isEmpty()) {
      destFilterQuery.minimumShouldMatch(1);
    }

    // Combine filters with the main query
    BoolQueryBuilder outgoingQuery;
    if (!sourceFilterQuery.should().isEmpty()) {
      outgoingQuery = QueryBuilders.boolQuery();
      outgoingQuery.filter(query);
      outgoingQuery.filter(sourceFilterQuery);
    } else {
      outgoingQuery = null;
    }

    BoolQueryBuilder incomingQuery;
    if (!destFilterQuery.should().isEmpty()) {
      incomingQuery = QueryBuilders.boolQuery();
      incomingQuery.filter(query);
      incomingQuery.filter(destFilterQuery);
    } else {
      incomingQuery = null;
    }

    // Use CompletableFutures to run queries in parallel
    List<CompletableFuture<List<SearchResponse>>> futures = new ArrayList<>();

    // Add outgoing query future if applicable
    if (outgoingQuery != null) {
      CompletableFuture<List<SearchResponse>> outgoingFuture =
          CompletableFuture.supplyAsync(
              () -> {
                log.debug("Starting parallel outgoing query execution");
                return executeQueryWithLimit(
                    opContext,
                    outgoingQuery,
                    pageSize,
                    exploreMultiplePaths,
                    entitiesPerHopLimit,
                    "outgoing",
                    originalEntityUrns);
              });
      futures.add(outgoingFuture);
    }

    // Add incoming query future if applicable
    if (incomingQuery != null) {
      CompletableFuture<List<SearchResponse>> incomingFuture =
          CompletableFuture.supplyAsync(
              () -> {
                log.debug("Starting parallel incoming query execution");
                return executeQueryWithLimit(
                    opContext,
                    incomingQuery,
                    pageSize,
                    exploreMultiplePaths,
                    entitiesPerHopLimit,
                    "incoming",
                    originalEntityUrns);
              });
      futures.add(incomingFuture);
    }

    // No queries to execute
    if (futures.isEmpty()) {
      return Stream.empty();
    }

    try {
      // Wait for all futures to complete
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

      // Add timeout based on configuration
      CompletableFuture<Void> futureWithTimeout =
          allFutures.orTimeout(config.getSearch().getGraph().getTimeoutSeconds(), TimeUnit.SECONDS);

      // Block until all futures complete or timeout
      futureWithTimeout.join();

      // Collect all results from all futures
      List<SearchResponse> allResults =
          futures.stream()
              .map(CompletableFuture::join)
              .flatMap(List::stream)
              .collect(Collectors.toList());

      log.debug("All parallel queries completed, collected {} total results", allResults.size());

      // Return stream of results
      return allResults.stream();

    } catch (Exception e) {
      log.error("Error executing parallel lineage queries", e);
      throw new ESQueryException("Failed to execute lineage queries", e);
    }
  }

  /**
   * Executes a search query with adaptive pagination, dynamically adjusting page size to
   * efficiently discover lineage relationships while respecting entity exploration limits.
   *
   * <p>This method implements an intelligent pagination strategy that:
   *
   * <ul>
   *   <li>Starts with an initial page size calculated based on the number of input entities and the
   *       desired entities per hop limit
   *   <li>Dynamically increases page size to explore more relationships
   *   <li>Stops when all input entities have reached their exploration limits
   * </ul>
   *
   * @param opContext The operation context containing metadata and configuration for the search
   * @param query The base Elasticsearch query builder to execute
   * @param pageSize The max page size if different then global max
   * @param exploreMultiplePaths Flag to determine if multiple paths to the same entity should be
   *     explored
   * @param entitiesPerHopLimit Optional limit on the number of entities to discover per input
   *     entity
   * @param direction A descriptive string indicating the relationship direction (for logging)
   * @param originalEntityUrns The set of input entity URNs to explore relationships from
   * @return A list of SearchResponse objects containing the discovered lineage relationships
   * @throws ESQueryException if there are issues executing the Elasticsearch search requests
   *     <p>Pagination Strategy Details:
   *     <ul>
   *       <li>Initial page size = min(2 * entitiesPerHopLimit * number of input URNs, max page
   *           size)
   *       <li>Page size increases exponentially when more entities need to be discovered
   *       <li>Ensures no single request exceeds the maximum configured page size
   *     </ul>
   *     <p>Example scenarios:
   *     <pre>
   * // Scenario 1: Multiple input entities with hop limit
   * executeQueryWithLimit(context, query, 10, false, 5, "outgoing", inputUrns)
   *
   * // Scenario 2: Single input entity without hop limit
   * executeQueryWithLimit(context, query, 100, true, null, "incoming", singleInputUrn)
   * </pre>
   *
   * @see #processResponseForEntityLimits Processing method for entity limit tracking
   * @see #createSearchAfterRequest Search request creation method
   */
  private List<SearchResponse> executeQueryWithLimit(
      OperationContext opContext,
      QueryBuilder query,
      int pageSize,
      boolean exploreMultiplePaths,
      Integer entitiesPerHopLimit,
      String direction,
      Set<Urn> originalEntityUrns) {

    // Determine maximum page size
    int maxPageSize =
        Math.min(pageSize, graphServiceConfig.getLimit().getResults().getApiDefault());

    // Initial page size calculation
    int currentPageSize = maxPageSize;
    if (entitiesPerHopLimit != null) {
      // Calculate initial page size: 2 * entitiesPerHopLimit * number of original entity URNs
      currentPageSize = Math.min(2 * entitiesPerHopLimit * originalEntityUrns.size(), maxPageSize);
      log.debug(
          "{} direction: initial page size calculated as {} (limit: {}, input urns: {})",
          direction,
          currentPageSize,
          entitiesPerHopLimit,
          originalEntityUrns.size());
    }

    List<SearchResponse> results = new ArrayList<>();

    // Track count of entities discovered per input entity
    Map<Urn, Set<Urn>> entitiesPerInputUrn = new HashMap<>();
    originalEntityUrns.forEach(urn -> entitiesPerInputUrn.put(urn, new HashSet<>()));

    // Track which input URNs have reached their limit
    Map<Urn, Boolean> inputUrnLimitReached = new HashMap<>();
    originalEntityUrns.forEach(urn -> inputUrnLimitReached.put(urn, false));

    // Initial request with calculated page size
    SearchRequest nextRequest = createSearchAfterRequest(opContext, query, currentPageSize, null);
    Object[] searchAfter = null;
    int iterationCount = 0;

    while (nextRequest != null) {
      // Check if all input URNs have reached their limit
      if (inputUrnLimitReached.values().stream().allMatch(Boolean::booleanValue)) {
        log.debug(
            "{} direction: all input URNs have reached their limits, stopping pagination",
            direction);
        break;
      }

      try {
        // Execute the current request
        SearchResponse response = executeSearchRequest(opContext, nextRequest);
        results.add(response);

        // Process entities in this response
        processResponseForEntityLimits(
            response,
            originalEntityUrns,
            entitiesPerInputUrn,
            inputUrnLimitReached,
            exploreMultiplePaths,
            entitiesPerHopLimit);

        // Check if we should continue
        SearchHit[] hits = response.getHits().getHits();
        if (hits.length == 0) {
          log.debug("{} direction: no more results, stopping pagination", direction);
          break;
        }

        // Prepare for the next page
        SearchHit lastHit = hits[hits.length - 1];
        searchAfter = lastHit.getSortValues();

        if (hits.length < currentPageSize) {
          log.debug("{} direction: no more results, incomplete page", direction);
          break;
        }

        // Adaptive page size strategy for scenarios with entitiesPerHopLimit
        if (entitiesPerHopLimit != null && currentPageSize <= maxPageSize) {
          boolean needMoreEntities =
              inputUrnLimitReached.values().stream().noneMatch(Boolean::booleanValue)
                  && entitiesPerInputUrn.values().stream()
                      .anyMatch(set -> set.size() < entitiesPerHopLimit);

          // Increase page size exponentially, but not beyond max
          if (needMoreEntities) {
            iterationCount++;
            currentPageSize =
                Math.min(
                    maxPageSize,
                    Math.max(
                        currentPageSize * 2, // Double the current page size
                        entitiesPerHopLimit * originalEntityUrns.size() * (1 << iterationCount)));
            log.debug(
                "{} direction: increasing page size to {} (iteration: {})",
                direction,
                currentPageSize,
                iterationCount);
          }
        }

        nextRequest = createSearchAfterRequest(opContext, query, currentPageSize, searchAfter);

      } catch (Exception e) {
        log.error("{} direction: error executing search request", direction, e);
        throw new ESQueryException("Failed to execute search request", e);
      }
    }

    // Log final results and entity discovery
    log.debug("{} direction: completed, collected {} results", direction, results.size());
    log.debug(
        "{} direction: entities discovered per input urn: {}",
        direction,
        entitiesPerInputUrn.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size())));

    return results;
  }

  private void processResponseForEntityLimits(
      SearchResponse response,
      Set<Urn> originalEntityUrns,
      Map<Urn, Set<Urn>> entitiesPerInputUrn,
      Map<Urn, Boolean> inputUrnLimitReached,
      boolean exploreMultiplePaths,
      Integer entitiesPerHopLimit) {

    SearchHit[] hits = response.getHits().getHits();

    for (SearchHit hit : hits) {
      Map<String, Object> document = hit.getSourceAsMap();

      // Extract source and destination URNs using utility class
      Urn sourceUrn =
          UrnExtractionUtils.extractUrnFromNestedFieldSafely(
              document, GraphQueryConstants.SOURCE, "source");
      Urn destinationUrn =
          UrnExtractionUtils.extractUrnFromNestedFieldSafely(
              document, GraphQueryConstants.DESTINATION, "destination");

      // Skip if either URN extraction failed
      if (sourceUrn == null || destinationUrn == null) {
        continue;
      }

      // Skip self-edges
      if (sourceUrn.equals(destinationUrn)) {
        log.debug("Skipping a self-edge on {}", sourceUrn);
        continue;
      }

      // Determine which input entity this relationship belongs to
      Urn inputUrn = null;
      Urn newEntityUrn = null;

      if (originalEntityUrns.contains(sourceUrn)) {
        inputUrn = sourceUrn;

        // This is an outgoing relationship from an input entity
        if (!originalEntityUrns.contains(destinationUrn)) {
          newEntityUrn = destinationUrn;
        }
      } else if (originalEntityUrns.contains(destinationUrn)) {
        inputUrn = destinationUrn;

        // This is an incoming relationship to an input entity
        if (!originalEntityUrns.contains(sourceUrn)) {
          newEntityUrn = sourceUrn;
        }
      }

      // If we found a new entity and have an input entity it relates to
      if (inputUrn != null && newEntityUrn != null) {
        Set<Urn> discoveredEntities =
            entitiesPerInputUrn.computeIfAbsent(inputUrn, k -> new HashSet<>());

        // If we're not exploring multiple paths and we've already seen this entity for this input,
        // skip
        if (!exploreMultiplePaths && discoveredEntities.contains(newEntityUrn)) {
          continue;
        }

        // Add the new entity to the set for this input
        discoveredEntities.add(newEntityUrn);

        // Check if this input entity has reached its limit
        if (entitiesPerHopLimit != null && discoveredEntities.size() >= entitiesPerHopLimit) {
          inputUrnLimitReached.put(inputUrn, true);
          log.debug(
              "Input urn {} has reached the entity limit of {}", inputUrn, entitiesPerHopLimit);
        }
      }
    }

    // Log progress
    log.debug(
        "Current entity counts per input urn: {}",
        entitiesPerInputUrn.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size())));

    log.debug(
        "Input urns that reached their limits: {}",
        inputUrnLimitReached.entrySet().stream()
            .filter(Map.Entry::getValue)
            .map(e -> e.getKey().toString())
            .collect(Collectors.toList()));
  }

  /**
   * Creates an Elasticsearch search request with search_after pagination parameters.
   *
   * <p>This method constructs a search request configured for efficient pagination using
   * Elasticsearch's search_after feature. The request is set up with a two-level sort: first by
   * document score (descending) to prioritize more relevant results, then by document ID
   * (ascending) to ensure stable pagination order even when scores are equal.
   *
   * <p>The search_after parameter, when provided, allows the request to fetch the next page of
   * results after the last document from the previous page, enabling deep pagination without the
   * performance issues of traditional offset-based pagination.
   *
   * @param query The Elasticsearch query to execute. This should be a fully-formed query with all
   *     necessary filters already applied.
   * @param pageSize The number of documents to retrieve in this search request. Controls the size
   *     of each page in the pagination process.
   * @param searchAfter The sort values from the last document of the previous page, used to specify
   *     where this search should continue from. Should be null for the first page of results.
   * @return A configured SearchRequest object ready to be executed against Elasticsearch. The
   *     request includes the query, sort order, pagination parameters, and is targeted at the graph
   *     index.
   */
  private SearchRequest createSearchAfterRequest(
      OperationContext opContext, QueryBuilder query, int pageSize, Object[] searchAfter) {

    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.query(query);
    searchSourceBuilder.size(pageSize);

    // Sort by `via` existence first, then by unique edge for stable pagination
    searchSourceBuilder.sort(
        SortBuilders.fieldSort(EDGE_FIELD_VIA).order(SortOrder.ASC).missing("_last"));
    searchSourceBuilder.sort(
        SortBuilders.fieldSort(EDGE_FIELD_UPDATED_ON).order(SortOrder.DESC).missing("_first"));
    searchSourceBuilder.sort(
        SortBuilders.fieldSort(EDGE_FIELD_CREATED_ON).order(SortOrder.DESC).missing("_first"));
    KEY_SORTS.forEach(
        sort -> {
          if (Objects.requireNonNull(sort.getValue())
              == com.linkedin.metadata.query.filter.SortOrder.DESCENDING) {
            searchSourceBuilder.sort(SortBuilders.fieldSort(sort.getFirst()).order(SortOrder.DESC));
          } else {
            searchSourceBuilder.sort(SortBuilders.fieldSort(sort.getFirst()).order(SortOrder.ASC));
          }
        });

    // Add search_after if provided
    if (searchAfter != null) {
      searchSourceBuilder.searchAfter(searchAfter);
    }

    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(
        opContext.getSearchContext().getIndexConvention().getIndexName(INDEX_NAME));

    return searchRequest;
  }

  private SearchResponse executeSearchRequest(OperationContext opContext, SearchRequest request) {
    return opContext.withSpan(
        "esLineageGroupByQuery",
        () -> {
          try {
            if (metricUtils != null)
              metricUtils.increment(
                  this.getClass(), GraphQueryConstants.SEARCH_EXECUTIONS_METRIC, 1);
            return getClient().search(request, RequestOptions.DEFAULT);
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "esLineageGroupByQuery"));
  }

  /**
   * Get lineage relationships up to the maximum number of relationships specified in the impact
   * configuration. This method scrolls through results using scroll+slice until it reaches the
   * limit or exhausts all results.
   *
   * <p>If maxRelations is set to -1 or 0, it is treated as unlimited and only the time limit
   * applies.
   *
   * @param opContext The operation context
   * @param entityUrn The source entity URN
   * @param lineageGraphFilters The lineage graph filters
   * @param maxHops The maximum number of hops to traverse
   * @return A LineageResponse containing all relationships up to the maxRelations limit (or time
   *     limit if maxRelations is -1 or 0)
   */
  @WithSpan
  public LineageResponse getImpactLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int maxHops) {

    // Validate that PIT is enabled for impact analysis
    if (!config.getSearch().getGraph().isPointInTimeCreationEnabled()) {
      throw new IllegalStateException(
          "Point-in-Time creation is required for impact analysis queries. "
              + "Please enable ELASTICSEARCH_SEARCH_GRAPH_POINT_IN_TIME_CREATION_ENABLED or set "
              + "elasticsearch.search.graph.pointInTimeCreationEnabled to true.");
    }

    // Get the maxRelations limit from configuration
    // -1 or 0 means unlimited (only bound by time limit)
    int maxRelations = config.getSearch().getGraph().getImpact().getMaxRelations();
    boolean allowPartialResults = config.getSearch().getGraph().getImpact().isPartialResults();
    boolean isMaxRelationsUnlimited = (maxRelations <= 0);

    Map<Urn, LineageRelationship> result = new HashMap<>();
    long currentTime = System.currentTimeMillis();
    long totalTimeoutMs = config.getSearch().getGraph().getTimeoutSeconds() * 1000L;

    // Calculate time reservation for second query phase (search query after graph traversal)
    // When partialResults is enabled, we reserve time for the subsequent search query.
    // We don't know in advance whether we'll hit timeout or maxRelations limit first, so we
    // always reserve time when partialResults is enabled to ensure there's time for the second
    // query.
    long reservedTimeForSearchQuery = 0L;
    if (allowPartialResults) {
      double searchQueryTimeReservation =
          config.getSearch().getGraph().getImpact().getSearchQueryTimeReservation();
      // Default to 20% if not configured (0.0) or invalid (negative or >= 1.0)
      if (searchQueryTimeReservation <= 0.0 || searchQueryTimeReservation >= 1.0) {
        searchQueryTimeReservation = 0.2;
      }
      reservedTimeForSearchQuery = (long) (totalTimeoutMs * searchQueryTimeReservation);
      // Ensure minimum reservation of 100ms to avoid rounding down to 0 for very small timeouts
      // This guarantees there's always some time reserved for the second query phase
      long minReservationMs = 100L;
      if (reservedTimeForSearchQuery < minReservationMs && totalTimeoutMs >= minReservationMs) {
        reservedTimeForSearchQuery = minReservationMs;
      }
      log.debug(
          "Reserving {} ms ({}%) of total timeout {} ms for second query phase when partial results are enabled",
          reservedTimeForSearchQuery, searchQueryTimeReservation * 100, totalTimeoutMs);
    }
    long remainingTime = totalTimeoutMs - reservedTimeForSearchQuery;
    long timeoutTime = currentTime + remainingTime;
    boolean isPartial = false;

    // Do a Level-order BFS
    Set<Urn> visitedEntities = ConcurrentHashMap.newKeySet();
    visitedEntities.add(entityUrn);
    Set<Urn> viaEntities = ConcurrentHashMap.newKeySet();
    ThreadSafePathStore existingPaths = new ThreadSafePathStore();
    List<Urn> currentLevel = ImmutableList.of(entityUrn);

    for (int i = 0; i < maxHops; i++) {
      if (currentLevel.isEmpty()) {
        break;
      }

      if (remainingTime < 0) {
        if (allowPartialResults) {
          log.warn(
              "Timed out while fetching lineage for {} with direction {}, maxHops {}. Returning partial results. {} ms reserved for second query phase.",
              entityUrn,
              lineageGraphFilters.getLineageDirection(),
              maxHops,
              reservedTimeForSearchQuery);
          isPartial = true;
          break;
        } else {
          log.error(
              "Timed out while fetching lineage for {} with direction {}, maxHops {}. Operation exceeded the configured timeout.",
              entityUrn,
              lineageGraphFilters.getLineageDirection(),
              maxHops);
          throw new IllegalStateException(
              String.format(
                  "Lineage operation timed out after %d seconds. Entity: %s, Direction: %s, MaxHops: %d. Consider increasing the timeout or set partialResults to true to return partial results.",
                  config.getSearch().getGraph().getTimeoutSeconds(),
                  entityUrn,
                  lineageGraphFilters.getLineageDirection(),
                  maxHops));
        }
      }

      // About to get lineage for `currentLevel`: annotate with `explored`
      currentLevel.forEach(
          urn -> Optional.ofNullable(result.get(urn)).ifPresent(rel -> rel.setExplored(true)));

      // Do one hop on the lineage graph
      // Note: maxRelations is the original total limit, but we pass the remaining capacity
      // to the scroll methods to ensure accurate limit checking at each level
      currentLevel =
          processOneHopLineageWithMaxRelations(
                  opContext,
                  currentLevel,
                  remainingTime,
                  maxHops,
                  lineageGraphFilters,
                  visitedEntities,
                  viaEntities,
                  existingPaths,
                  result,
                  i,
                  maxRelations,
                  allowPartialResults)
              .collect(Collectors.toList());

      currentTime = System.currentTimeMillis();
      remainingTime = timeoutTime - currentTime;

      // Check if we've reached the maxRelations limit (skip if unlimited, i.e., -1)
      if (!isMaxRelationsUnlimited && result.size() >= maxRelations) {
        if (allowPartialResults) {
          log.warn(
              "Reached maxRelations limit {} for {} with direction {}, maxHops {}. Returning partial results.",
              maxRelations,
              entityUrn,
              lineageGraphFilters.getLineageDirection(),
              maxHops);
          isPartial = true;
          break;
        } else {
          log.error(
              "Reached maxRelations limit {} for {} with direction {}, maxHops {}. This indicates the data exceeds the configured limit.",
              maxRelations,
              entityUrn,
              lineageGraphFilters.getLineageDirection(),
              maxHops);
          throw new IllegalStateException(
              String.format(
                  "Lineage results exceeded the configured maxRelations limit of %d. Entity: %s, Direction: %s, MaxHops: %d. Consider reducing maxHops or increasing the maxRelations limit, or set partialResults to true to return partial results.",
                  maxRelations, entityUrn, lineageGraphFilters.getLineageDirection(), maxHops));
        }
      }

      // Early termination if no new entities to process
      if (currentLevel.isEmpty()) {
        break;
      }
    }

    List<LineageRelationship> resultList = new ArrayList<>(result.values());
    return new LineageResponse(resultList.size(), resultList, isPartial);
  }

  private Stream<Urn> processOneHopLineageWithMaxRelations(
      @Nonnull OperationContext opContext,
      List<Urn> currentLevel,
      long remainingTime,
      int maxHops,
      LineageGraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      ThreadSafePathStore existingPaths,
      Map<Urn, LineageRelationship> result,
      int i,
      int maxRelations,
      boolean allowPartialResults) {

    // Do one hop on the lineage graph
    int numHops = i + 1; // Zero indexed for loop counter, one indexed count
    int remainingHops = maxHops - numHops;

    // Calculate remaining capacity and pass it to scroll methods
    // This ensures scroll methods check against the actual remaining capacity, not the original
    // total limit
    // If maxRelations is -1 or 0 (unlimited), pass -1 to indicate no limit
    int remainingCapacity = (maxRelations <= 0) ? -1 : Math.max(0, maxRelations - result.size());
    List<LineageRelationship> oneHopRelationships =
        getLineageRelationshipsWithMaxRelations(
            opContext,
            currentLevel,
            graphFilters,
            visitedEntities,
            viaEntities,
            numHops,
            remainingHops,
            remainingTime,
            existingPaths,
            remainingCapacity,
            allowPartialResults);

    for (LineageRelationship oneHopRelnship : oneHopRelationships) {
      if (result.containsKey(oneHopRelnship.getEntity())) {
        log.debug("Urn encountered again during graph walk {}", oneHopRelnship.getEntity());
        result.put(
            oneHopRelnship.getEntity(),
            mergeLineageRelationships(result.get(oneHopRelnship.getEntity()), oneHopRelnship));
      } else {
        result.put(oneHopRelnship.getEntity(), oneHopRelnship);
      }
    }
    return oneHopRelationships.stream().map(LineageRelationship::getEntity);
  }

  // Get 1-hop lineage relationships with timeout
  @WithSpan
  private List<LineageRelationship> getLineageRelationshipsWithMaxRelations(
      @Nonnull final OperationContext opContext,
      @Nonnull List<Urn> entityUrns,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      long remainingTime,
      ThreadSafePathStore existingPaths,
      int maxRelations,
      boolean allowPartialResults) {

    Map<String, Set<Urn>> urnsPerEntityType =
        entityUrns.stream().collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));

    QueryBuilder finalQuery = getLineageQuery(opContext, urnsPerEntityType, lineageGraphFilters);

    // Use scroll search to get all results up to maxRelations
    return scrollLineageSearchWithMaxRelations(
        opContext,
        finalQuery,
        lineageGraphFilters,
        visitedEntities,
        viaEntities,
        numHops,
        remainingHops,
        existingPaths,
        maxRelations,
        remainingTime,
        new HashSet<>(entityUrns),
        allowPartialResults);
  }

  /**
   * Search through lineage results up to the maximum number of relationships using PIT-based search
   * with slicing for parallel processing.
   *
   * @param maxRelations The remaining capacity for relationships (decremented from original limit)
   * @param allowPartialResults If true, return partial results on timeout or maxRelations instead
   *     of throwing
   */
  private List<LineageRelationship> scrollLineageSearchWithMaxRelations(
      @Nonnull OperationContext opContext,
      @Nonnull QueryBuilder query,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      int maxRelations, // This is the REMAINING capacity, not the original total limit
      long remainingTime,
      Set<Urn> entityUrns,
      boolean allowPartialResults) {

    int defaultPageSize = graphServiceConfig.getLimit().getResults().getApiDefault();
    int slices = Math.max(2, config.getSearch().getGraph().getImpact().getSlices());

    return searchWithSlices(
        opContext,
        query,
        lineageGraphFilters,
        visitedEntities,
        viaEntities,
        numHops,
        remainingHops,
        existingPaths,
        maxRelations,
        defaultPageSize,
        slices,
        remainingTime,
        entityUrns,
        allowPartialResults);
  }

  /**
   * Process slice futures with common error handling and result collection logic. This method is
   * reused by both PIT and scroll implementations.
   *
   * @param sliceFutures The list of futures for each slice
   * @param remainingTime Remaining time in milliseconds
   * @param allowPartialResults If true, return partial results on timeout instead of throwing
   */
  protected List<LineageRelationship> processSliceFutures(
      List<CompletableFuture<List<LineageRelationship>>> sliceFutures,
      long remainingTime,
      boolean allowPartialResults) {

    List<LineageRelationship> allRelationships = Collections.synchronizedList(new ArrayList<>());

    try {
      // Process each future one by one with proper timeout management
      // This ensures immediate exception detection and proper resource cleanup
      for (int i = 0; i < sliceFutures.size(); i++) {
        CompletableFuture<List<LineageRelationship>> future = sliceFutures.get(i);

        // Calculate remaining time for this future
        long startTime = System.currentTimeMillis();
        long futureTimeout = Math.max(1, remainingTime / 1000); // Ensure at least 1 second

        try {
          // Wait for this specific future to complete
          List<LineageRelationship> sliceResults = future.get(futureTimeout, TimeUnit.SECONDS);
          allRelationships.addAll(sliceResults);

          // Update remaining time for next iteration
          long elapsed = System.currentTimeMillis() - startTime;
          remainingTime -= elapsed;

          // If we're out of time, break early
          if (remainingTime <= 0) {
            if (allowPartialResults) {
              log.warn(
                  "Out of time, stopping slice processing after {} slices. Returning partial results.",
                  i + 1);
            } else {
              log.warn("Out of time, stopping slice processing after {} slices", i + 1);
            }
            break;
          }

        } catch (TimeoutException e) {
          log.warn("Slice {} timed out after {} seconds", i, futureTimeout);
          // Cancel all futures to prevent resource leaks
          sliceFutures.forEach(f -> f.cancel(true));
          if (allowPartialResults) {
            log.warn(
                "Returning partial results after slice {} timed out. {} relationships collected so far.",
                i,
                allRelationships.size());
            break;
          } else {
            log.error("Slice {} timed out after {} seconds", i, futureTimeout);
            throw new RuntimeException(
                "Slice " + i + " timed out after " + futureTimeout + " seconds", e);
          }
        } catch (Exception e) {
          log.error("Slice {} failed with exception", i, e);
          // Cancel other futures to prevent resource leaks
          sliceFutures.forEach(f -> f.cancel(true));
          // Re-throw the original exception to preserve the exception chain
          if (e instanceof RuntimeException) {
            throw e;
          } else {
            throw new RuntimeException("Slice " + i + " failed", e);
          }
        }
      }
    } catch (Exception e) {
      // If we have partial results enabled and collected some results, return them
      if (allowPartialResults && !allRelationships.isEmpty()) {
        log.warn(
            "Error during slice-based search, but returning partial results. {} relationships collected so far.",
            allRelationships.size(),
            e);
        return allRelationships;
      }
      // Otherwise, throw the exception
      log.error("Error during slice-based search", e);
      throw new RuntimeException("Failed to execute slice-based search", e);
    }

    return allRelationships;
  }

  @Override
  public void cleanupPointInTime(String pitId) {
    ESUtils.cleanupPointInTime(getClient(), pitId, "API Request");
  }
}
