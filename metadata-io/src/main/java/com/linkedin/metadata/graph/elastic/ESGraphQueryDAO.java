package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.*;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.*;
import static com.linkedin.metadata.search.utils.ESUtils.applyResultLimit;
import static com.linkedin.metadata.search.utils.ESUtils.queryOptimize;

import com.datahub.util.exception.ESQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry.EdgeInfo;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
@RequiredArgsConstructor
public class ESGraphQueryDAO {

  private final RestHighLevelClient client;
  private final LineageRegistry lineageRegistry;
  private final IndexConvention indexConvention;

  private final ElasticSearchConfiguration config;

  static final String SOURCE = "source";
  static final String DESTINATION = "destination";
  static final String RELATIONSHIP_TYPE = "relationshipType";
  static final String SOURCE_TYPE = SOURCE + ".entityType";
  static final String SOURCE_URN = SOURCE + ".urn";
  static final String DESTINATION_TYPE = DESTINATION + ".entityType";
  static final String DESTINATION_URN = DESTINATION + ".urn";
  static final String SEARCH_EXECUTIONS_METRIC = "num_elasticSearch_reads";
  static final String CREATED_ON = "createdOn";
  static final String CREATED_ACTOR = "createdActor";
  static final String UPDATED_ON = "updatedOn";
  static final String UPDATED_ACTOR = "updatedActor";
  static final String PROPERTIES = "properties";
  static final String UI = "UI";

  private static void addFilterToQueryBuilder(
      @Nonnull Filter filter, @Nullable String node, BoolQueryBuilder rootQuery) {
    BoolQueryBuilder orQuery = new BoolQueryBuilder();
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      final BoolQueryBuilder andQuery = new BoolQueryBuilder();
      final List<Criterion> criterionArray = conjunction.getAnd();
      if (!criterionArray.stream()
          .allMatch(criterion -> Condition.EQUAL.equals(criterion.getCondition()))) {
        throw new RuntimeException(
            "Currently Elastic query filter only supports EQUAL condition " + criterionArray);
      }
      criterionArray.forEach(
          criterion ->
              andQuery.filter(
                  QueryBuilders.termsQuery(
                      (node == null ? "" : node + ".") + criterion.getField(),
                      criterion.getValues())));
      orQuery.should(andQuery);
    }
    if (!orQuery.should().isEmpty()) {
      orQuery.minimumShouldMatch(1);
    }
    rootQuery.filter(orQuery);
  }

  private SearchResponse executeLineageSearchQuery(
      @Nonnull OperationContext opContext,
      @Nonnull final QueryBuilder query,
      final int offset,
      final int count) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = sharedSourceBuilder(query, offset, count);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    return opContext.withSpan(
        "esQuery",
        () -> {
          try {
            MetricUtils.counter(this.getClass(), SEARCH_EXECUTIONS_METRIC).inc();
            return client.search(searchRequest, RequestOptions.DEFAULT);
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "esQuery"));
  }

  private SearchSourceBuilder sharedSourceBuilder(
      @Nonnull final QueryBuilder query, final int offset, final int count) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(applyResultLimit(config, count));

    searchSourceBuilder.query(query);
    if (config.getSearch().getGraph().isBoostViaNodes()) {
      addViaNodeBoostQuery(searchSourceBuilder);
    }
    return searchSourceBuilder;
  }

  private static BoolQueryBuilder getAggregationFilter(
      Pair<String, EdgeInfo> pair, RelationshipDirection direction) {
    BoolQueryBuilder subFilter = QueryBuilders.boolQuery();
    TermQueryBuilder relationshipTypeTerm =
        QueryBuilders.termQuery(RELATIONSHIP_TYPE, pair.getValue().getType()).caseInsensitive(true);
    subFilter.filter(relationshipTypeTerm);

    String sourceType;
    String destinationType;
    if (direction.equals(RelationshipDirection.OUTGOING)) {
      sourceType = pair.getKey();
      destinationType = pair.getValue().getOpposingEntityType();
    } else {
      sourceType = pair.getValue().getOpposingEntityType();
      destinationType = pair.getKey();
    }

    TermQueryBuilder sourceTypeTerm =
        QueryBuilders.termQuery(SOURCE_TYPE, sourceType).caseInsensitive(true);
    subFilter.filter(sourceTypeTerm);
    TermQueryBuilder destinationTypeTerm =
        QueryBuilders.termQuery(DESTINATION_TYPE, destinationType).caseInsensitive(true);
    subFilter.filter(destinationTypeTerm);
    return subFilter;
  }

  public SearchResponse getSearchResponse(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      final int offset,
      final int count) {
    BoolQueryBuilder finalQuery =
        buildQuery(opContext, config.getSearch().getGraph(), graphFilters);

    return executeLineageSearchQuery(opContext, finalQuery, offset, count);
  }

  public static BoolQueryBuilder buildQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphQueryConfiguration graphQueryConfiguration,
      @Nonnull final GraphFilters graphFilters) {
    return buildQuery(opContext, graphQueryConfiguration, graphFilters, null);
  }

  public static BoolQueryBuilder buildQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphQueryConfiguration graphQueryConfiguration,
      @Nonnull final GraphFilters graphFilters,
      @Nullable final String lifecycleOwner) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    final RelationshipDirection relationshipDirection = graphFilters.getRelationshipDirection();

    // set source filter
    String sourceNode =
        relationshipDirection == RelationshipDirection.OUTGOING ? SOURCE : DESTINATION;
    if (graphFilters.isSourceTypesFilterEnabled()) {
      finalQuery.filter(
          QueryBuilders.termsQuery(sourceNode + ".entityType", graphFilters.getSourceTypes()));
    }
    Optional.ofNullable(graphFilters.getSourceEntityFilter())
        .ifPresent(
            sourceEntityFilter ->
                addFilterToQueryBuilder(sourceEntityFilter, sourceNode, finalQuery));

    // set destination filter
    String destinationNode =
        relationshipDirection == RelationshipDirection.OUTGOING ? DESTINATION : SOURCE;
    if (graphFilters.isDestinationTypesFilterEnabled()) {
      finalQuery.filter(
          QueryBuilders.termsQuery(
              destinationNode + ".entityType", graphFilters.getDestinationTypes()));
    }
    Optional.ofNullable(graphFilters.getDestinationEntityFilter())
        .ifPresent(
            destinationEntityFilter ->
                addFilterToQueryBuilder(destinationEntityFilter, destinationNode, finalQuery));

    // set relationship filter
    if (!graphFilters.getRelationshipTypes().isEmpty()) {
      BoolQueryBuilder relationshipQuery = QueryBuilders.boolQuery();
      graphFilters
          .getRelationshipTypes()
          .forEach(
              relationshipType ->
                  relationshipQuery.should(
                      QueryBuilders.termQuery(RELATIONSHIP_TYPE, relationshipType)));
      relationshipQuery.minimumShouldMatch(1);

      finalQuery.filter(relationshipQuery);
    }

    // general filter
    Optional.ofNullable(graphFilters.getRelationshipFilter())
        .map(RelationshipFilter::getOr)
        .ifPresent(or -> addFilterToQueryBuilder(new Filter().setOr(or), null, finalQuery));

    if (lifecycleOwner != null) {
      finalQuery.filter(QueryBuilders.termQuery(EDGE_FIELD_LIFECYCLE_OWNER, lifecycleOwner));
    }
    if (!Optional.ofNullable(opContext.getSearchContext().getSearchFlags().isIncludeSoftDeleted())
        .orElse(false)) {
      applyExcludeSoftDelete(graphQueryConfiguration, finalQuery);
    }

    return finalQuery;
  }

  @WithSpan
  public LineageResponse getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      LineageGraphFilters lineageGraphFilters,
      int offset,
      int count,
      int maxHops) {
    Map<Urn, LineageRelationship> result = new HashMap<>();
    long currentTime = System.currentTimeMillis();
    long remainingTime = config.getSearch().getGraph().getTimeoutSeconds() * 1000;
    boolean exploreMultiplePaths = config.getSearch().getGraph().isEnableMultiPathSearch();
    long timeoutTime = currentTime + remainingTime;

    // Do a Level-order BFS
    Set<Urn> visitedEntities = ConcurrentHashMap.newKeySet();
    visitedEntities.add(entityUrn);
    Set<Urn> viaEntities = ConcurrentHashMap.newKeySet();
    Map<Urn, UrnArrayArray> existingPaths = new HashMap<>();
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
    LineageResponse response = new LineageResponse(resultList.size(), resultList);

    List<LineageRelationship> subList;
    if (offset >= response.getTotal()) {
      subList = Collections.emptyList();
    } else {
      subList =
          response
              .getLineageRelationships()
              .subList(offset, Math.min(offset + count, response.getTotal()));
    }

    return new LineageResponse(response.getTotal(), subList);
  }

  private Stream<Urn> processOneHopLineage(
      @Nonnull OperationContext opContext,
      List<Urn> currentLevel,
      Long remainingTime,
      int maxHops,
      LineageGraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      Map<Urn, UrnArrayArray> existingPaths,
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
                                        || platformMatches(
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

  private boolean platformMatches(Urn urn, UrnArray platforms) {
    return platforms.stream()
        .anyMatch(
            platform ->
                DataPlatformInstanceUtils.getDataPlatform(urn)
                    .toString()
                    .equals(platform.toString()));
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
  private LineageRelationship mergeLineageRelationships(
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
      UrnArrayArray copyPaths =
          new UrnArrayArray(
              existingRelationship.getPaths().size() + newRelationship.getPaths().size());
      copyPaths.addAll(existingRelationship.getPaths());
      copyPaths.addAll(newRelationship.getPaths());
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
      Map<Urn, UrnArrayArray> existingPaths,
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
      Map<Urn, UrnArrayArray> existingPaths,
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
              opContext, finalQuery, 0, config.getSearch().getLimit().getResults().getMax());
      return extractRelationships(
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
          buildLineageGraphFiltersQuery(entityType, urns, lineageGraphFilters)
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

  @VisibleForTesting
  static QueryBuilder getLineageQueryForEntityType(
      @Nonnull Set<Urn> urns, @Nonnull Set<EdgeInfo> lineageEdges) {
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    Map<RelationshipDirection, Set<EdgeInfo>> edgesByDirection =
        lineageEdges.stream()
            .collect(Collectors.groupingBy(EdgeInfo::getDirection, Collectors.toSet()));

    Set<EdgeInfo> outgoingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.OUTGOING, Collections.emptySet());
    if (!outgoingEdges.isEmpty()) {
      query.should(getOutGoingEdgeQuery(urns, outgoingEdges));
    }

    Set<EdgeInfo> incomingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.INCOMING, Collections.emptySet());
    if (!incomingEdges.isEmpty()) {
      query.should(getIncomingEdgeQuery(urns, incomingEdges));
    }

    if (!incomingEdges.isEmpty() || !outgoingEdges.isEmpty()) {
      query.minimumShouldMatch(1);
    }

    return query;
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
        config.getSearch().getLimit().getResults().getMax()); // Will rescore all results
    sourceBuilder.addRescorer(queryRescorerBuilder);
  }

  /**
   * Adds an individual relationship edge to a running set of unique paths to each node in the
   * graph.
   *
   * <p>Specifically, this method updates 'existingPaths', which is a map of an entity urn
   * representing a node in the lineage graph to the full paths that can be traversed to reach it
   * from a the origin node for which lineage was requested.
   *
   * <p>This method strictly assumes that edges are being added IN ORDER, level-by-level working
   * outwards from the originally requested source node. If edges are added to the path set in an
   * out of order manner, then the paths to a given node may be partial / incomplete.
   *
   * <p>Note that calling this method twice with the same edge is not safe. It will result in
   * duplicate paths being appended into the list of paths to the provided child urn.
   *
   * @param existingPaths a running set of unique, uni-directional paths to each node in the graph
   *     starting from the original root node for which lineage was requested.
   * @param parentUrn the "parent" node (or source node) in the edge to add. This is a logical
   *     source node in a uni-directional path from the source to the destination node. Note that
   *     this is NOT always the URN corresponding to the "source" field that is physically stored
   *     inside the Graph Store.
   * @param childUrn the "child" node (or dest node) in the edge to add. This is a logical dest node
   *     in a uni-directional path from the source to the destination node. Note that this is NOT
   *     always the URN corresponding to the "destination" field that is physically stored inside
   *     the Graph Store.
   */
  @VisibleForTesting
  static void addEdgeToPaths(
      @Nonnull final Map<Urn, UrnArrayArray> existingPaths,
      @Nonnull final Urn parentUrn,
      @Nonnull final Urn childUrn) {
    addEdgeToPaths(existingPaths, parentUrn, null, childUrn);
  }

  private static boolean containsCycle(final UrnArray path) {
    Set<Urn> urnSet = path.stream().collect(Collectors.toUnmodifiableSet());
    // path contains a cycle if any urn is repeated twice
    return (path.size() != urnSet.size());
  }

  static boolean addEdgeToPaths(
      @Nonnull final Map<Urn, UrnArrayArray> existingPaths,
      @Nonnull final Urn parentUrn,
      final Urn viaUrn,
      @Nonnull final Urn childUrn) {
    boolean edgeAdded = false;
    // Collect all full-paths to this child node. This is what will be returned.
    UrnArrayArray pathsToParent = existingPaths.get(parentUrn);
    if (pathsToParent != null && !pathsToParent.isEmpty()) {
      // If there are existing paths to this parent node, then we attempt
      // to append the child to each of the existing paths (lengthen it).
      // We then store this as a separate, unique path associated with the child.
      for (UrnArray pathToParent : pathsToParent) {
        if (containsCycle(pathToParent)) {
          log.debug("Skipping extending path {} because it contains a cycle", pathToParent);
          continue;
        }
        UrnArray pathToChild = clonePath(pathToParent);
        if (viaUrn != null) {
          pathToChild.add(viaUrn);
        }
        pathToChild.add(childUrn);
        // Save these paths to the global structure for easy access on future iterations.
        existingPaths.putIfAbsent(childUrn, new UrnArrayArray());
        UrnArrayArray existingPathsToChild = existingPaths.get(childUrn);
        boolean dupExists = false;
        for (UrnArray existingPathToChild : existingPathsToChild) {
          if (existingPathToChild.equals(pathToChild)) {
            dupExists = true;
          }
        }
        if (!dupExists) {
          existingPathsToChild.add(pathToChild);
          edgeAdded = true;
        }
      }
    } else {
      // No existing paths to this parent urn. Let's create a new path to the child!
      UrnArray pathToChild = new UrnArray();
      if (viaUrn == null) {
        pathToChild.addAll(ImmutableList.of(parentUrn, childUrn));
      } else {
        pathToChild.addAll(ImmutableList.of(parentUrn, viaUrn, childUrn));
      }
      // Save these paths to the global structure for easy access on future iterations.
      existingPaths.putIfAbsent(childUrn, new UrnArrayArray());
      existingPaths.get(childUrn).add(pathToChild);
      edgeAdded = true;
    }
    return edgeAdded;
  }

  // Given set of edges and the search response, extract all valid edges that originate from the
  // input entityUrns
  @WithSpan
  private static List<LineageRelationship> extractRelationships(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull SearchResponse searchResponse,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      Map<Urn, UrnArrayArray> existingPaths,
      boolean exploreMultiplePaths) {
    try {
      Map<Urn, LineageRelationship> lineageRelationshipMap = new HashMap<>();
      final SearchHit[] hits = searchResponse.getHits().getHits();
      log.debug("numHits: {}, numHops {}, remainingHops {}", hits.length, numHops, remainingHops);
      int index = -1;
      for (SearchHit hit : hits) {
        processSearchHit(
            hit,
            entityUrns,
            index,
            exploreMultiplePaths,
            visitedEntities,
            lineageGraphFilters,
            existingPaths,
            numHops,
            false,
            lineageRelationshipMap,
            viaEntities);
      }

      List<LineageRelationship> result = new ArrayList<>(lineageRelationshipMap.values());
      log.debug("Number of lineage relationships in list: {}", result.size());
      return result;
    } catch (Exception e) {
      // This exception handler merely exists to log the exception at an appropriate point and
      // rethrow
      log.error("Caught exception", e);
      throw e;
    }
  }

  private static void processSearchHit(
      SearchHit hit,
      Set<Urn> entityUrns,
      int index,
      boolean exploreMultiplePaths,
      Set<Urn> visitedEntities,
      LineageGraphFilters lineageGraphFilters,
      Map<Urn, UrnArrayArray> existingPaths,
      int numHops,
      boolean truncatedChildren,
      Map<Urn, LineageRelationship> lineageRelationshipMap,
      Set<Urn> viaEntities) {
    index++;
    // Extract fields
    final Map<String, Object> document = hit.getSourceAsMap();
    final Urn sourceUrn =
        UrnUtils.getUrn(((Map<String, Object>) document.get(SOURCE)).get("urn").toString());
    final Urn destinationUrn =
        UrnUtils.getUrn(((Map<String, Object>) document.get(DESTINATION)).get("urn").toString());
    final String type = document.get(RELATIONSHIP_TYPE).toString();
    if (sourceUrn.equals(destinationUrn)) {
      log.debug("Skipping a self-edge of type {} on {}", type, sourceUrn);
      return;
    }
    final Number createdOnNumber = (Number) document.getOrDefault(CREATED_ON, null);
    final Long createdOn = createdOnNumber != null ? createdOnNumber.longValue() : null;
    final Number updatedOnNumber = (Number) document.getOrDefault(UPDATED_ON, null);
    final Long updatedOn = updatedOnNumber != null ? updatedOnNumber.longValue() : null;
    final String createdActorString = (String) document.getOrDefault(CREATED_ACTOR, null);
    final Urn createdActor =
        createdActorString == null ? null : UrnUtils.getUrn(createdActorString);
    final String updatedActorString = (String) document.getOrDefault(UPDATED_ACTOR, null);
    final Urn updatedActor =
        updatedActorString == null ? null : UrnUtils.getUrn(updatedActorString);
    final Map<String, Object> properties;
    if (document.containsKey(PROPERTIES) && document.get(PROPERTIES) instanceof Map) {
      properties = (Map<String, Object>) document.get(PROPERTIES);
    } else {
      properties = Collections.emptyMap();
    }
    boolean isManual = properties.containsKey(SOURCE) && properties.get(SOURCE).equals("UI");
    Urn viaEntity = null;
    String viaContent = (String) document.getOrDefault(EDGE_FIELD_VIA, null);
    if (viaContent != null) {
      try {
        viaEntity = Urn.createFromString(viaContent);
      } catch (Exception e) {
        log.warn(
            "Failed to parse urn from via entity {}, will swallow exception and continue...",
            viaContent);
      }
    }
    log.debug("{}: viaEntity {}", index, viaEntity);

    // Potential outgoing edge
    if (entityUrns.contains(sourceUrn)) {
      processOutgoingEdge(
          entityUrns,
          sourceUrn,
          index,
          exploreMultiplePaths,
          visitedEntities,
          destinationUrn,
          lineageGraphFilters,
          type,
          existingPaths,
          viaEntity,
          numHops,
          createdOn,
          createdActor,
          updatedOn,
          updatedActor,
          isManual,
          truncatedChildren,
          lineageRelationshipMap,
          viaEntities);
    }

    // Potential incoming edge
    if (entityUrns.contains(destinationUrn)) {
      processIncomingEdge(
          entityUrns,
          sourceUrn,
          exploreMultiplePaths,
          visitedEntities,
          destinationUrn,
          lineageGraphFilters,
          type,
          existingPaths,
          viaEntity,
          numHops,
          createdOn,
          createdActor,
          updatedOn,
          updatedActor,
          isManual,
          truncatedChildren,
          lineageRelationshipMap,
          viaEntities);
    }
  }

  private static void processOutgoingEdge(
      Set<Urn> entityUrns,
      Urn sourceUrn,
      int index,
      boolean exploreMultiplePaths,
      Set<Urn> visitedEntities,
      Urn destinationUrn,
      LineageGraphFilters lineageGraphFilters,
      String type,
      Map<Urn, UrnArrayArray> existingPaths,
      Urn viaEntity,
      int numHops,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      boolean isManual,
      boolean truncatedChildren,
      Map<Urn, LineageRelationship> lineageRelationshipMap,
      Set<Urn> viaEntities) {
    if (entityUrns.contains(sourceUrn)) {
      log.debug("{}: entity urns contains source urn {}", index, sourceUrn);
      // Skip if already visited or if we're exploring multiple paths
      // Skip if edge is not a valid outgoing edge
      if ((exploreMultiplePaths || !visitedEntities.contains(destinationUrn))
          && lineageGraphFilters.containsEdgeInfo(
              sourceUrn.getEntityType(),
              new EdgeInfo(type, RelationshipDirection.OUTGOING, destinationUrn.getEntityType()))) {

        if (visitedEntities.contains(destinationUrn)) {
          log.debug("Found a second path to the same urn {}", destinationUrn);
        }
        // Append the edge to a set of unique graph paths.
        if (addEdgeToPaths(existingPaths, sourceUrn, viaEntity, destinationUrn)) {
          final LineageRelationship relationship =
              createLineageRelationship(
                  type,
                  destinationUrn,
                  numHops,
                  existingPaths.getOrDefault(destinationUrn, new UrnArrayArray()),
                  // Fetch the paths to the next level entity.
                  createdOn,
                  createdActor,
                  updatedOn,
                  updatedActor,
                  isManual,
                  truncatedChildren);
          log.debug("Adding relationship {} to urn {}", relationship, destinationUrn);
          lineageRelationshipMap.put(relationship.getEntity(), relationship);
          if ((viaEntity != null) && (!viaEntities.contains(viaEntity))) {
            UrnArrayArray viaPaths = getViaPaths(existingPaths, destinationUrn, viaEntity);
            LineageRelationship viaRelationship =
                createLineageRelationship(
                    type,
                    viaEntity,
                    numHops,
                    viaPaths,
                    createdOn,
                    createdActor,
                    updatedOn,
                    updatedActor,
                    isManual,
                    truncatedChildren);
            viaEntities.add(viaEntity);
            lineageRelationshipMap.put(viaRelationship.getEntity(), viaRelationship);
            log.debug("Adding via entity {} with paths {}", viaEntity, viaPaths);
          }
        }
        visitedEntities.add(destinationUrn);
      }
    }
  }

  private static void processIncomingEdge(
      Set<Urn> entityUrns,
      Urn sourceUrn,
      boolean exploreMultiplePaths,
      Set<Urn> visitedEntities,
      Urn destinationUrn,
      LineageGraphFilters lineageGraphFilters,
      String type,
      Map<Urn, UrnArrayArray> existingPaths,
      Urn viaEntity,
      int numHops,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      boolean isManual,
      boolean truncatedChildren,
      Map<Urn, LineageRelationship> lineageRelationshipMap,
      Set<Urn> viaEntities) {
    if (entityUrns.contains(destinationUrn)) {
      // Skip if already visited or if we're exploring multiple paths
      // Skip if edge is not a valid outgoing edge
      log.debug("entity urns contains destination urn {}", destinationUrn);
      if ((exploreMultiplePaths || !visitedEntities.contains(sourceUrn))
          && lineageGraphFilters.containsEdgeInfo(
              destinationUrn.getEntityType(),
              new EdgeInfo(type, RelationshipDirection.INCOMING, sourceUrn.getEntityType()))) {
        if (visitedEntities.contains(sourceUrn)) {
          log.debug("Found a second path to the same urn {}", sourceUrn);
        }
        visitedEntities.add(sourceUrn);
        // Append the edge to a set of unique graph paths.
        if (addEdgeToPaths(existingPaths, destinationUrn, viaEntity, sourceUrn)) {
          log.debug("Adding incoming edge: {}, {}, {}", destinationUrn, viaEntity, sourceUrn);
          final LineageRelationship relationship =
              createLineageRelationship(
                  type,
                  sourceUrn,
                  numHops,
                  existingPaths.getOrDefault(sourceUrn, new UrnArrayArray()),
                  // Fetch the paths to the next level entity.
                  createdOn,
                  createdActor,
                  updatedOn,
                  updatedActor,
                  isManual,
                  truncatedChildren);
          log.debug("Adding relationship {} to urn {}", relationship, sourceUrn);
          lineageRelationshipMap.put(relationship.getEntity(), relationship);
          if ((viaEntity != null) && (!viaEntities.contains(viaEntity))) {
            UrnArrayArray viaPaths = getViaPaths(existingPaths, sourceUrn, viaEntity);
            viaEntities.add(viaEntity);
            LineageRelationship viaRelationship =
                createLineageRelationship(
                    type,
                    viaEntity,
                    numHops,
                    viaPaths,
                    createdOn,
                    createdActor,
                    updatedOn,
                    updatedActor,
                    isManual,
                    truncatedChildren);
            lineageRelationshipMap.put(viaRelationship.getEntity(), viaRelationship);
            log.debug("Adding via relationship {} to urn {}", viaRelationship, viaEntity);
          }
        }
      }
    }
  }

  private static UrnArrayArray getViaPaths(
      Map<Urn, UrnArrayArray> existingPaths, Urn destinationUrn, Urn viaEntity) {
    UrnArrayArray destinationPaths =
        existingPaths.getOrDefault(destinationUrn, new UrnArrayArray());
    UrnArrayArray viaPaths = new UrnArrayArray();
    for (UrnArray destPath : destinationPaths) {
      UrnArray viaPath = new UrnArray();
      for (Urn urn : destPath) {
        viaPath.add(urn);
        if (urn.equals(viaEntity)) {
          break;
        }
      }
      viaPaths.add(viaPath);
    }
    return viaPaths;
  }

  private static LineageRelationship createLineageRelationship(
      @Nonnull final String type,
      @Nonnull final Urn entityUrn,
      final int numHops,
      @Nonnull final UrnArrayArray paths,
      @Nullable final Long createdOn,
      @Nullable final Urn createdActor,
      @Nullable final Long updatedOn,
      @Nullable final Urn updatedActor,
      final boolean isManual,
      final boolean truncatedChildren) {
    final LineageRelationship relationship =
        new LineageRelationship()
            .setType(type)
            .setEntity(entityUrn)
            .setDegree(numHops)
            .setDegrees(new IntegerArray(ImmutableList.of(numHops)))
            .setPaths(paths);
    if (createdOn != null) {
      relationship.setCreatedOn(createdOn);
    }
    if (createdActor != null) {
      relationship.setCreatedActor(createdActor);
    }
    if (updatedOn != null) {
      relationship.setUpdatedOn(updatedOn);
    }
    if (updatedActor != null) {
      relationship.setUpdatedActor(updatedActor);
    }
    relationship.setIsManual(isManual);
    relationship.setTruncatedChildren(truncatedChildren);
    return relationship;
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
      Map<Urn, UrnArrayArray> existingPaths,
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
            config.getSearch().getLimit().getResults().getMax(),
            exploreMultiplePaths,
            entitiesPerHopLimit,
            entityUrns);

    // Process the stream of responses to extract relationships
    return extractRelationshipsFromSearchResponses(
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

  private static BoolQueryBuilder getOutGoingEdgeQuery(
      @Nonnull Set<Urn> urns, @Nonnull Set<EdgeInfo> outgoingEdges) {
    BoolQueryBuilder outgoingEdgeQuery = QueryBuilders.boolQuery();
    outgoingEdgeQuery.filter(buildUrnFilters(urns, SOURCE));
    outgoingEdgeQuery.filter(buildEdgeFilters(outgoingEdges));
    return outgoingEdgeQuery;
  }

  private static BoolQueryBuilder getIncomingEdgeQuery(
      @Nonnull Set<Urn> urns, Set<EdgeInfo> incomingEdges) {
    BoolQueryBuilder incomingEdgeQuery = QueryBuilders.boolQuery();
    incomingEdgeQuery.filter(buildUrnFilters(urns, DESTINATION));
    incomingEdgeQuery.filter(buildEdgeFilters(incomingEdges));
    return incomingEdgeQuery;
  }

  private static UrnArray clonePath(final UrnArray basePath) {
    try {
      return basePath.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(String.format("Failed to clone path %s", basePath), e);
    }
  }

  private static QueryBuilder buildUrnFilters(@Nonnull Set<Urn> urns, @Nonnull String prefix) {
    // dedup urns while preserving order
    LinkedHashSet<String> urnSet = new LinkedHashSet<>();
    urns.forEach(urn -> urnSet.add(urn.toString()));
    return QueryBuilders.termsQuery(prefix + ".urn", urnSet);
  }

  private static QueryBuilder buildEdgeFilters(@Nonnull Set<EdgeInfo> edgeInfos) {
    return QueryBuilders.termsQuery(
        "relationshipType",
        edgeInfos.stream().map(EdgeInfo::getType).distinct().collect(Collectors.toList()));
  }

  @Value
  public static class LineageResponse {
    int total;
    List<LineageRelationship> lineageRelationships;
  }

  public SearchResponse getSearchResponse(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      int count) {

    BoolQueryBuilder finalQuery =
        buildQuery(opContext, config.getSearch().getGraph(), graphFilters);

    return executeScrollSearchQuery(opContext, finalQuery, sortCriteria, scrollId, count);
  }

  private SearchResponse executeScrollSearchQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final QueryBuilder query,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      final int count) {

    Object[] sort = null;
    if (scrollId != null) {
      SearchAfterWrapper searchAfterWrapper = SearchAfterWrapper.fromScrollId(scrollId);
      sort = searchAfterWrapper.getSort();
    }

    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.size(applyResultLimit(config, count));
    searchSourceBuilder.query(query);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, List.of(), false);
    searchRequest.source(searchSourceBuilder);
    ESUtils.setSearchAfter(searchSourceBuilder, sort, null, null);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    return opContext.withSpan(
        "esQuery",
        () -> {
          try {
            MetricUtils.counter(this.getClass(), SEARCH_EXECUTIONS_METRIC).inc();
            return client.search(searchRequest, RequestOptions.DEFAULT);
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "esQuery"));
  }

  private static void applyExcludeSoftDelete(
      GraphQueryConfiguration graphQueryConfiguration, BoolQueryBuilder boolQueryBuilder) {
    if (graphQueryConfiguration.isGraphStatusEnabled()) {
      Arrays.stream(EdgeUrnType.values())
          .map(
              edgeUrnType ->
                  QueryBuilders.termsQuery(
                      GraphFilterUtils.getUrnStatusFieldName(edgeUrnType), "true"))
          .filter(statusQuery -> !boolQueryBuilder.mustNot().contains(statusQuery))
          .forEach(boolQueryBuilder::mustNot);
    }
  }

  /**
   * @param lineageGraphFilters lineage graph filters being applied
   * @return elasticsearch boolean filter query
   */
  private Optional<BoolQueryBuilder> buildLineageGraphFiltersQuery(
      @Nonnull String entityType,
      @Nonnull Set<Urn> urns,
      @Nonnull LineageGraphFilters lineageGraphFilters) {

    if (urns.stream().anyMatch(urn -> !entityType.equals(urn.getEntityType()))) {
      throw new IllegalArgumentException("Urns must be of the same entity type.");
    }

    Set<EdgeInfo> edgeInfo = lineageGraphFilters.getEdgeInfo(lineageRegistry, entityType);

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
                  .filter(QueryBuilders.termsQuery(SOURCE_URN, entityUrns))
                  .filter(QueryBuilders.termQuery(RELATIONSHIP_TYPE, relationshipType));

          // Use termsQuery for multiple types
          outgoingQuery.filter(QueryBuilders.termsQuery(DESTINATION_TYPE, entityTypes));

          mainQuery.should(outgoingQuery);
        } else if (direction == RelationshipDirection.INCOMING) {
          BoolQueryBuilder incomingQuery =
              QueryBuilders.boolQuery()
                  .filter(QueryBuilders.termsQuery(DESTINATION_URN, entityUrns))
                  .filter(QueryBuilders.termQuery(RELATIONSHIP_TYPE, relationshipType));

          // Use termsQuery for multiple types
          incomingQuery.filter(QueryBuilders.termsQuery(SOURCE_TYPE, entityTypes));

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
                    getAggregationFilter(pair, RelationshipDirection.OUTGOING)));

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
                destFilterQuery.should(getAggregationFilter(pair, RelationshipDirection.INCOMING)));

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
    int maxPageSize = Math.min(pageSize, config.getSearch().getLimit().getResults().getMax());

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
    SearchRequest nextRequest = createSearchAfterRequest(query, currentPageSize, null);
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

        nextRequest = createSearchAfterRequest(query, currentPageSize, searchAfter);

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

      // Extract source and destination URNs
      String sourceUrnStr = ((Map<String, Object>) document.get(SOURCE)).get("urn").toString();
      String destinationUrnStr =
          ((Map<String, Object>) document.get(DESTINATION)).get("urn").toString();
      Urn sourceUrn = UrnUtils.getUrn(sourceUrnStr);
      Urn destinationUrn = UrnUtils.getUrn(destinationUrnStr);

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
      QueryBuilder query, int pageSize, Object[] searchAfter) {

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
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    return searchRequest;
  }

  private SearchResponse executeSearchRequest(OperationContext opContext, SearchRequest request) {
    return opContext.withSpan(
        "esLineageGroupByQuery",
        () -> {
          try {
            MetricUtils.counter(this.getClass(), SEARCH_EXECUTIONS_METRIC).inc();
            return client.search(request, RequestOptions.DEFAULT);
          } catch (Exception e) {
            log.error("Search query failed", e);
            throw new ESQueryException("Search query failed:", e);
          }
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "esLineageGroupByQuery"));
  }

  /** Extracts lineage relationships from search responses, respecting per-entity hop limits */
  @WithSpan
  private static List<LineageRelationship> extractRelationshipsFromSearchResponses(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull Stream<SearchResponse> searchResponses,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      Map<Urn, UrnArrayArray> existingPaths,
      boolean exploreMultiplePaths,
      @Nullable Integer entitiesPerHopLimit) {

    try {
      Map<Urn, LineageRelationship> lineageRelationshipMap = new ConcurrentHashMap<>();

      // Track count of entities discovered per input entity
      Map<Urn, Set<Urn>> entitiesPerInputUrn = new HashMap<>();
      entityUrns.forEach(urn -> entitiesPerInputUrn.put(urn, new HashSet<>()));

      // Process all search responses first to gather all possible edges
      Map<Urn, Set<SearchHit>> hitsPerInputEntity = new HashMap<>();
      entityUrns.forEach(urn -> hitsPerInputEntity.put(urn, new HashSet<>()));

      // Organize hits by which input entity they relate to
      searchResponses.forEach(
          searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
              Map<String, Object> document = hit.getSourceAsMap();

              // Extract source and destination URNs
              String sourceUrnStr =
                  ((Map<String, Object>) document.get(SOURCE)).get("urn").toString();
              String destinationUrnStr =
                  ((Map<String, Object>) document.get(DESTINATION)).get("urn").toString();
              Urn sourceUrn = UrnUtils.getUrn(sourceUrnStr);
              Urn destinationUrn = UrnUtils.getUrn(destinationUrnStr);

              // Assign hit to input entity it relates to
              if (entityUrns.contains(sourceUrn)) {
                hitsPerInputEntity.get(sourceUrn).add(hit);
              }

              if (entityUrns.contains(destinationUrn)) {
                hitsPerInputEntity.get(destinationUrn).add(hit);
              }
            }
          });

      // Now process hits per input entity, respecting individual limits
      for (Urn inputUrn : entityUrns) {
        Set<SearchHit> hits = hitsPerInputEntity.get(inputUrn);
        Set<Urn> discoveredEntities = entitiesPerInputUrn.get(inputUrn);

        // Process hits for this input entity
        int index = -1;
        for (SearchHit hit : hits) {
          index++;
          Map<String, Object> document = hit.getSourceAsMap();

          // Extract source and destination URNs
          String sourceUrnStr = ((Map<String, Object>) document.get(SOURCE)).get("urn").toString();
          String destinationUrnStr =
              ((Map<String, Object>) document.get(DESTINATION)).get("urn").toString();
          Urn sourceUrn = UrnUtils.getUrn(sourceUrnStr);
          Urn destinationUrn = UrnUtils.getUrn(destinationUrnStr);

          // Determine which entity is the "new" entity in this relationship
          Urn newEntityUrn = null;
          boolean isOutgoing = false;

          if (sourceUrn.equals(inputUrn) && !entityUrns.contains(destinationUrn)) {
            // Outgoing edge to a new entity
            newEntityUrn = destinationUrn;
            isOutgoing = true;
          } else if (destinationUrn.equals(inputUrn) && !entityUrns.contains(sourceUrn)) {
            // Incoming edge from a new entity
            newEntityUrn = sourceUrn;
            isOutgoing = false;
          }

          // If we found a new entity and haven't reached the limit yet
          if (newEntityUrn != null
              && (entitiesPerHopLimit == null || discoveredEntities.size() < entitiesPerHopLimit)) {

            // Add to discovered entities for this input
            discoveredEntities.add(newEntityUrn);

            // Process the hit to extract the relationship
            boolean truncatedResults = false; // We'll handle truncation separately

            // Process the search hit based on direction
            if (isOutgoing) {
              processOutgoingEdge(
                  entityUrns,
                  sourceUrn,
                  index,
                  exploreMultiplePaths,
                  visitedEntities,
                  destinationUrn,
                  lineageGraphFilters,
                  document.get(RELATIONSHIP_TYPE).toString(),
                  existingPaths,
                  extractViaEntity(document),
                  numHops,
                  extractCreatedOn(document),
                  extractCreatedActor(document),
                  extractUpdatedOn(document),
                  extractUpdatedActor(document),
                  isManual(document),
                  truncatedResults,
                  lineageRelationshipMap,
                  viaEntities);
            } else {
              processIncomingEdge(
                  entityUrns,
                  sourceUrn,
                  exploreMultiplePaths,
                  visitedEntities,
                  destinationUrn,
                  lineageGraphFilters,
                  document.get(RELATIONSHIP_TYPE).toString(),
                  existingPaths,
                  extractViaEntity(document),
                  numHops,
                  extractCreatedOn(document),
                  extractCreatedActor(document),
                  extractUpdatedOn(document),
                  extractUpdatedActor(document),
                  isManual(document),
                  truncatedResults,
                  lineageRelationshipMap,
                  viaEntities);
            }
          }
        }

        // Mark relationships as truncated if we hit the limit
        if (entitiesPerHopLimit != null && discoveredEntities.size() >= entitiesPerHopLimit) {
          for (LineageRelationship relationship : lineageRelationshipMap.values()) {
            // Check if this relationship belongs to the current input entity
            if (isRelationshipConnectedToInput(relationship, inputUrn, existingPaths)) {
              relationship.setTruncatedChildren(true);
            }
          }
        }
      }

      List<LineageRelationship> result = new ArrayList<>(lineageRelationshipMap.values());
      log.debug("Number of lineage relationships in list: {}", result.size());
      return result;
    } catch (Exception e) {
      log.error("Caught exception while processing search responses", e);
      throw e;
    }
  }

  // Helper methods for extracting fields from documents
  private static Urn extractViaEntity(Map<String, Object> document) {
    String viaContent = (String) document.getOrDefault(EDGE_FIELD_VIA, null);
    if (viaContent != null) {
      try {
        return Urn.createFromString(viaContent);
      } catch (Exception e) {
        log.warn("Failed to parse urn from via entity {}", viaContent);
      }
    }
    return null;
  }

  private static Long extractCreatedOn(Map<String, Object> document) {
    Number createdOnNumber = (Number) document.getOrDefault(CREATED_ON, null);
    return createdOnNumber != null ? createdOnNumber.longValue() : null;
  }

  private static Urn extractCreatedActor(Map<String, Object> document) {
    String createdActorString = (String) document.getOrDefault(CREATED_ACTOR, null);
    return createdActorString == null ? null : UrnUtils.getUrn(createdActorString);
  }

  private static Long extractUpdatedOn(Map<String, Object> document) {
    Number updatedOnNumber = (Number) document.getOrDefault(UPDATED_ON, null);
    return updatedOnNumber != null ? updatedOnNumber.longValue() : null;
  }

  private static Urn extractUpdatedActor(Map<String, Object> document) {
    String updatedActorString = (String) document.getOrDefault(UPDATED_ACTOR, null);
    return updatedActorString == null ? null : UrnUtils.getUrn(updatedActorString);
  }

  private static boolean isManual(Map<String, Object> document) {
    Map<String, Object> properties;
    if (document.containsKey(PROPERTIES) && document.get(PROPERTIES) instanceof Map) {
      properties = (Map<String, Object>) document.get(PROPERTIES);
    } else {
      properties = Collections.emptyMap();
    }
    return properties.containsKey(SOURCE) && properties.get(SOURCE).equals(UI);
  }

  // Check if a relationship is connected to a specific input entity
  private static boolean isRelationshipConnectedToInput(
      LineageRelationship relationship, Urn inputUrn, Map<Urn, UrnArrayArray> existingPaths) {

    // Check if the relationship's paths include the input URN
    UrnArrayArray paths = existingPaths.get(relationship.getEntity());
    if (paths == null) {
      return false;
    }

    for (UrnArray path : paths) {
      if (path.contains(inputUrn)) {
        return true;
      }
    }

    return false;
  }
}
