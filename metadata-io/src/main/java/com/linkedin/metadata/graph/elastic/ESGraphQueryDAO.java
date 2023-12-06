package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.*;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry.EdgeInfo;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
@RequiredArgsConstructor
public class ESGraphQueryDAO {

  private final RestHighLevelClient client;
  private final LineageRegistry lineageRegistry;
  private final IndexConvention indexConvention;

  private final GraphQueryConfiguration graphQueryConfiguration;

  static final String SOURCE = "source";
  static final String DESTINATION = "destination";
  static final String RELATIONSHIP_TYPE = "relationshipType";
  static final String SEARCH_EXECUTIONS_METRIC = "num_elasticSearch_reads";
  static final String CREATED_ON = "createdOn";
  static final String CREATED_ACTOR = "createdActor";
  static final String UPDATED_ON = "updatedOn";
  static final String UPDATED_ACTOR = "updatedActor";
  static final String PROPERTIES = "properties";
  static final String UI = "UI";

  @Nonnull
  public static void addFilterToQueryBuilder(
      @Nonnull Filter filter, String node, BoolQueryBuilder rootQuery) {
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
              andQuery.must(
                  QueryBuilders.termQuery(
                      node + "." + criterion.getField(), criterion.getValue())));
      orQuery.should(andQuery);
    }
    rootQuery.must(orQuery);
  }

  private SearchResponse executeSearchQuery(
      @Nonnull final QueryBuilder query, final int offset, final int count) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(count);

    searchSourceBuilder.query(query);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esQuery").time()) {
      MetricUtils.counter(this.getClass(), SEARCH_EXECUTIONS_METRIC).inc();
      return client.search(searchRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private SearchResponse executeSearchQuery(
      @Nonnull final QueryBuilder query,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nonnull String keepAlive,
      final int count) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);
    searchSourceBuilder.size(count);
    searchSourceBuilder.query(query);

    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esQuery").time()) {
      MetricUtils.counter(this.getClass(), SEARCH_EXECUTIONS_METRIC).inc();
      return client.search(searchRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  public SearchResponse getSearchResponse(
      @Nullable final List<String> sourceTypes,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      final int offset,
      final int count) {
    BoolQueryBuilder finalQuery =
        buildQuery(
            sourceTypes,
            sourceEntityFilter,
            destinationTypes,
            destinationEntityFilter,
            relationshipTypes,
            relationshipFilter);

    return executeSearchQuery(finalQuery, offset, count);
  }

  public static BoolQueryBuilder buildQuery(
      @Nullable final List<String> sourceTypes,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    // set source filter
    String sourceNode =
        relationshipDirection == RelationshipDirection.OUTGOING ? SOURCE : DESTINATION;
    if (sourceTypes != null && sourceTypes.size() > 0) {
      finalQuery.must(QueryBuilders.termsQuery(sourceNode + ".entityType", sourceTypes));
    }
    addFilterToQueryBuilder(sourceEntityFilter, sourceNode, finalQuery);

    // set destination filter
    String destinationNode =
        relationshipDirection == RelationshipDirection.OUTGOING ? DESTINATION : SOURCE;
    if (destinationTypes != null && destinationTypes.size() > 0) {
      finalQuery.must(QueryBuilders.termsQuery(destinationNode + ".entityType", destinationTypes));
    }
    addFilterToQueryBuilder(destinationEntityFilter, destinationNode, finalQuery);

    // set relationship filter
    if (relationshipTypes.size() > 0) {
      BoolQueryBuilder relationshipQuery = QueryBuilders.boolQuery();
      relationshipTypes.forEach(
          relationshipType ->
              relationshipQuery.should(
                  QueryBuilders.termQuery(RELATIONSHIP_TYPE, relationshipType)));
      finalQuery.must(relationshipQuery);
    }
    return finalQuery;
  }

  @WithSpan
  public LineageResponse getLineage(
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      GraphFilters graphFilters,
      int offset,
      int count,
      int maxHops,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    List<LineageRelationship> result = new ArrayList<>();
    long currentTime = System.currentTimeMillis();
    long remainingTime = graphQueryConfiguration.getTimeoutSeconds() * 1000;
    long timeoutTime = currentTime + remainingTime;

    // Do a Level-order BFS
    Set<Urn> visitedEntities = ConcurrentHashMap.newKeySet();
    visitedEntities.add(entityUrn);
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
            direction,
            maxHops);
        break;
      }

      // Do one hop on the lineage graph
      List<LineageRelationship> oneHopRelationships =
          getLineageRelationshipsInBatches(
              currentLevel,
              direction,
              graphFilters,
              visitedEntities,
              i + 1,
              remainingTime,
              existingPaths,
              startTimeMillis,
              endTimeMillis);
      result.addAll(oneHopRelationships);
      currentLevel =
          oneHopRelationships.stream()
              .map(LineageRelationship::getEntity)
              .collect(Collectors.toList());
      currentTime = System.currentTimeMillis();
      remainingTime = timeoutTime - currentTime;
    }
    LineageResponse response = new LineageResponse(result.size(), result);

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

  // Get 1-hop lineage relationships asynchronously in batches with timeout
  @WithSpan
  public List<LineageRelationship> getLineageRelationshipsInBatches(
      @Nonnull List<Urn> entityUrns,
      @Nonnull LineageDirection direction,
      GraphFilters graphFilters,
      Set<Urn> visitedEntities,
      int numHops,
      long remainingTime,
      Map<Urn, UrnArrayArray> existingPaths,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    List<List<Urn>> batches = Lists.partition(entityUrns, graphQueryConfiguration.getBatchSize());
    return ConcurrencyUtils.getAllCompleted(
            batches.stream()
                .map(
                    batchUrns ->
                        CompletableFuture.supplyAsync(
                            () ->
                                getLineageRelationships(
                                    batchUrns,
                                    direction,
                                    graphFilters,
                                    visitedEntities,
                                    numHops,
                                    existingPaths,
                                    startTimeMillis,
                                    endTimeMillis)))
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
      @Nonnull List<Urn> entityUrns,
      @Nonnull LineageDirection direction,
      GraphFilters graphFilters,
      Set<Urn> visitedEntities,
      int numHops,
      Map<Urn, UrnArrayArray> existingPaths,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    Map<String, List<Urn>> urnsPerEntityType =
        entityUrns.stream().collect(Collectors.groupingBy(Urn::getEntityType));
    Map<String, List<EdgeInfo>> edgesPerEntityType =
        urnsPerEntityType.keySet().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    entityType -> lineageRegistry.getLineageRelationships(entityType, direction)));
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    // Get all relation types relevant to the set of urns to hop from
    urnsPerEntityType.forEach(
        (entityType, urns) ->
            finalQuery.should(
                getQueryForLineage(
                    urns,
                    edgesPerEntityType.getOrDefault(entityType, Collections.emptyList()),
                    graphFilters,
                    startTimeMillis,
                    endTimeMillis)));
    SearchResponse response =
        executeSearchQuery(finalQuery, 0, graphQueryConfiguration.getMaxResult());
    Set<Urn> entityUrnSet = new HashSet<>(entityUrns);
    // Get all valid edges given the set of urns to hop from
    Set<Pair<String, EdgeInfo>> validEdges =
        edgesPerEntityType.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().stream().map(edgeInfo -> Pair.of(entry.getKey(), edgeInfo)))
            .collect(Collectors.toSet());
    return extractRelationships(
        entityUrnSet, response, validEdges, visitedEntities, numHops, existingPaths);
  }

  // Get search query for given list of edges and source urns
  @VisibleForTesting
  public static QueryBuilder getQueryForLineage(
      @Nonnull List<Urn> urns,
      @Nonnull List<EdgeInfo> lineageEdges,
      @Nonnull GraphFilters graphFilters,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    if (lineageEdges.isEmpty()) {
      return query;
    }
    Map<RelationshipDirection, List<EdgeInfo>> edgesByDirection =
        lineageEdges.stream().collect(Collectors.groupingBy(EdgeInfo::getDirection));

    List<EdgeInfo> outgoingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.OUTGOING, Collections.emptyList());
    if (!outgoingEdges.isEmpty()) {
      query.should(getOutGoingEdgeQuery(urns, outgoingEdges, graphFilters));
    }

    List<EdgeInfo> incomingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.INCOMING, Collections.emptyList());
    if (!incomingEdges.isEmpty()) {
      query.should(getIncomingEdgeQuery(urns, incomingEdges, graphFilters));
    }

    /*
     * Optional - Add edge filtering based on time windows.
     */
    if (startTimeMillis != null && endTimeMillis != null) {
      query.must(TimeFilterUtils.getEdgeTimeFilterQuery(startTimeMillis, endTimeMillis));
    } else {
      log.debug(
          String.format(
              "Empty time filter range provided: start time %s, end time: %s. Skipping application of time filters",
              startTimeMillis, endTimeMillis));
    }

    return query;
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
  public static void addEdgeToPaths(
      @Nonnull final Map<Urn, UrnArrayArray> existingPaths,
      @Nonnull final Urn parentUrn,
      @Nonnull final Urn childUrn) {
    // Collect all full-paths to this child node. This is what will be returned.
    UrnArrayArray pathsToParent = existingPaths.get(parentUrn);
    if (pathsToParent != null && pathsToParent.size() > 0) {
      // If there are existing paths to this parent node, then we attempt
      // to append the child to each of the existing paths (lengthen it).
      // We then store this as a separate, unique path associated with the child.
      for (final UrnArray pathToParent : pathsToParent) {
        UrnArray pathToChild = clonePath(pathToParent);
        pathToChild.add(childUrn);
        // Save these paths to the global structure for easy access on future iterations.
        existingPaths.putIfAbsent(childUrn, new UrnArrayArray());
        existingPaths.get(childUrn).add(pathToChild);
      }
    } else {
      // No existing paths to this parent urn. Let's create a new path to the child!
      UrnArray pathToChild = new UrnArray();
      pathToChild.addAll(ImmutableList.of(parentUrn, childUrn));
      // Save these paths to the global structure for easy access on future iterations.
      existingPaths.putIfAbsent(childUrn, new UrnArrayArray());
      existingPaths.get(childUrn).add(pathToChild);
    }
  }

  // Given set of edges and the search response, extract all valid edges that originate from the
  // input entityUrns
  @WithSpan
  private static List<LineageRelationship> extractRelationships(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull SearchResponse searchResponse,
      Set<Pair<String, EdgeInfo>> validEdges,
      Set<Urn> visitedEntities,
      int numHops,
      Map<Urn, UrnArrayArray> existingPaths) {
    final List<LineageRelationship> result = new LinkedList<>();
    final SearchHit[] hits = searchResponse.getHits().getHits();
    for (SearchHit hit : hits) {
      final Map<String, Object> document = hit.getSourceAsMap();
      final Urn sourceUrn =
          UrnUtils.getUrn(((Map<String, Object>) document.get(SOURCE)).get("urn").toString());
      final Urn destinationUrn =
          UrnUtils.getUrn(((Map<String, Object>) document.get(DESTINATION)).get("urn").toString());
      final String type = document.get(RELATIONSHIP_TYPE).toString();
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

      // Potential outgoing edge
      if (entityUrns.contains(sourceUrn)) {
        // Skip if already visited
        // Skip if edge is not a valid outgoing edge
        // TODO: Verify if this honors multiple paths to the same node.
        if (!visitedEntities.contains(destinationUrn)
            && validEdges.contains(
                Pair.of(
                    sourceUrn.getEntityType(),
                    new EdgeInfo(
                        type,
                        RelationshipDirection.OUTGOING,
                        destinationUrn.getEntityType().toLowerCase())))) {
          visitedEntities.add(destinationUrn);
          // Append the edge to a set of unique graph paths.
          addEdgeToPaths(existingPaths, sourceUrn, destinationUrn);
          final LineageRelationship relationship =
              createLineageRelationship(
                  type,
                  destinationUrn,
                  numHops,
                  existingPaths.getOrDefault(
                      destinationUrn,
                      new UrnArrayArray()), // Fetch the paths to the next level entity.
                  createdOn,
                  createdActor,
                  updatedOn,
                  updatedActor,
                  isManual);
          result.add(relationship);
        }
      }

      // Potential incoming edge
      if (entityUrns.contains(destinationUrn)) {
        // Skip if already visited
        // Skip if edge is not a valid outgoing edge
        // TODO: Verify if this honors multiple paths to the same node.
        if (!visitedEntities.contains(sourceUrn)
            && validEdges.contains(
                Pair.of(
                    destinationUrn.getEntityType(),
                    new EdgeInfo(
                        type,
                        RelationshipDirection.INCOMING,
                        sourceUrn.getEntityType().toLowerCase())))) {
          visitedEntities.add(sourceUrn);
          // Append the edge to a set of unique graph paths.
          addEdgeToPaths(existingPaths, destinationUrn, sourceUrn);
          final LineageRelationship relationship =
              createLineageRelationship(
                  type,
                  sourceUrn,
                  numHops,
                  existingPaths.getOrDefault(
                      sourceUrn, new UrnArrayArray()), // Fetch the paths to the next level entity.
                  createdOn,
                  createdActor,
                  updatedOn,
                  updatedActor,
                  isManual);
          result.add(relationship);
        }
      }
    }
    return result;
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
      final boolean isManual) {
    final LineageRelationship relationship =
        new LineageRelationship()
            .setType(type)
            .setEntity(entityUrn)
            .setDegree(numHops)
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
    return relationship;
  }

  private static BoolQueryBuilder getOutGoingEdgeQuery(
      @Nonnull List<Urn> urns,
      @Nonnull List<EdgeInfo> outgoingEdges,
      @Nonnull GraphFilters graphFilters) {
    BoolQueryBuilder outgoingEdgeQuery = QueryBuilders.boolQuery();
    outgoingEdgeQuery.must(buildUrnFilters(urns, SOURCE));
    outgoingEdgeQuery.must(buildEdgeFilters(outgoingEdges));
    outgoingEdgeQuery.must(buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), SOURCE));
    outgoingEdgeQuery.must(
        buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), DESTINATION));
    return outgoingEdgeQuery;
  }

  private static BoolQueryBuilder getIncomingEdgeQuery(
      @Nonnull List<Urn> urns, List<EdgeInfo> incomingEdges, @Nonnull GraphFilters graphFilters) {
    BoolQueryBuilder incomingEdgeQuery = QueryBuilders.boolQuery();
    incomingEdgeQuery.must(buildUrnFilters(urns, DESTINATION));
    incomingEdgeQuery.must(buildEdgeFilters(incomingEdges));
    incomingEdgeQuery.must(buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), SOURCE));
    incomingEdgeQuery.must(
        buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), DESTINATION));
    return incomingEdgeQuery;
  }

  private static UrnArray clonePath(final UrnArray basePath) {
    try {
      return basePath.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(String.format("Failed to clone path %s", basePath), e);
    }
  }

  private static QueryBuilder buildEntityTypesFilter(
      @Nonnull List<String> entityTypes, @Nonnull String prefix) {
    return QueryBuilders.termsQuery(
        prefix + ".entityType",
        entityTypes.stream().map(Object::toString).collect(Collectors.toList()));
  }

  private static QueryBuilder buildUrnFilters(@Nonnull List<Urn> urns, @Nonnull String prefix) {
    return QueryBuilders.termsQuery(
        prefix + ".urn", urns.stream().map(Object::toString).collect(Collectors.toList()));
  }

  private static QueryBuilder buildEdgeFilters(@Nonnull List<EdgeInfo> edgeInfos) {
    return QueryBuilders.termsQuery(
        "relationshipType",
        edgeInfos.stream().map(EdgeInfo::getType).distinct().collect(Collectors.toList()));
  }

  @Value
  public static class LineageResponse {
    int total;
    List<LineageRelationship> lineageRelationships;
  }
}
