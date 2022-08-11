package com.linkedin.metadata.graph.elastic;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry.EdgeInfo;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;


/**
 * A search DAO for Elasticsearch backend.
 */
@Slf4j
@RequiredArgsConstructor
public class ESGraphQueryDAO {

  private final RestHighLevelClient client;
  private final LineageRegistry lineageRegistry;
  private final IndexConvention indexConvention;

  private static final int MAX_ELASTIC_RESULT = 10000;
  private static final int BATCH_SIZE = 1000;
  private static final int TIMEOUT_SECS = 10;
  private static final String SOURCE = "source";
  private static final String DESTINATION = "destination";
  private static final String RELATIONSHIP_TYPE = "relationshipType";

  @Nonnull
  public static void addFilterToQueryBuilder(@Nonnull Filter filter, String node, BoolQueryBuilder rootQuery) {
    BoolQueryBuilder orQuery = new BoolQueryBuilder();
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      final BoolQueryBuilder andQuery = new BoolQueryBuilder();
      final List<Criterion> criterionArray = conjunction.getAnd();
      if (!criterionArray.stream().allMatch(criterion -> Condition.EQUAL.equals(criterion.getCondition()))) {
        throw new RuntimeException("Currently Elastic query filter only supports EQUAL condition " + criterionArray);
      }
      criterionArray.forEach(
          criterion -> andQuery.must(QueryBuilders.termQuery(node + "." + criterion.getField(), criterion.getValue())));
      orQuery.should(andQuery);
    }
    rootQuery.must(orQuery);
  }

  private SearchResponse executeSearchQuery(@Nonnull final QueryBuilder query, final int offset, final int count) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(count);

    searchSourceBuilder.query(query);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esQuery").time()) {
      return client.search(searchRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Search query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  public SearchResponse getSearchResponse(@Nullable final List<String> sourceTypes, @Nonnull final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes, @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes, @Nonnull final RelationshipFilter relationshipFilter,
      final int offset, final int count) {
    BoolQueryBuilder finalQuery =
        buildQuery(sourceTypes, sourceEntityFilter, destinationTypes, destinationEntityFilter, relationshipTypes,
            relationshipFilter);

    return executeSearchQuery(finalQuery, offset, count);
  }

  public static BoolQueryBuilder buildQuery(@Nullable final List<String> sourceTypes, @Nonnull final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes, @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes, @Nonnull final RelationshipFilter relationshipFilter) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    // set source filter
    String sourceNode = relationshipDirection == RelationshipDirection.OUTGOING ? SOURCE : DESTINATION;
    if (sourceTypes != null && sourceTypes.size() > 0) {
      finalQuery.must(QueryBuilders.termsQuery(sourceNode + ".entityType", sourceTypes));
    }
    addFilterToQueryBuilder(sourceEntityFilter, sourceNode, finalQuery);

    // set destination filter
    String destinationNode = relationshipDirection == RelationshipDirection.OUTGOING ? DESTINATION : SOURCE;
    if (destinationTypes != null && destinationTypes.size() > 0) {
      finalQuery.must(QueryBuilders.termsQuery(destinationNode + ".entityType", destinationTypes));
    }
    addFilterToQueryBuilder(destinationEntityFilter, destinationNode, finalQuery);

    // set relationship filter
    if (relationshipTypes.size() > 0) {
      BoolQueryBuilder relationshipQuery = QueryBuilders.boolQuery();
      relationshipTypes.forEach(
          relationshipType -> relationshipQuery.should(QueryBuilders.termQuery(RELATIONSHIP_TYPE, relationshipType)));
      finalQuery.must(relationshipQuery);
    }
    return finalQuery;
  }

  @WithSpan
  public LineageResponse getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction, GraphFilters graphFilters, int offset, int count,
      int maxHops) {
    List<LineageRelationship> result = new ArrayList<>();
    long currentTime = System.currentTimeMillis();
    long remainingTime = TIMEOUT_SECS * 1000;
    long timeoutTime = currentTime + remainingTime;

    // Do a Level-order BFS
    Set<Urn> visitedEntities = ConcurrentHashMap.newKeySet();
    visitedEntities.add(entityUrn);
    List<Urn> currentLevel = ImmutableList.of(entityUrn);

    for (int i = 0; i < maxHops; i++) {
      if (currentLevel.isEmpty()) {
        break;
      }

      if (remainingTime < 0) {
        log.info("Timed out while fetching lineage for {} with direction {}, maxHops {}. Returning results so far",
            entityUrn, direction, maxHops);
        break;
      }

      // Do one hop on the lineage graph
      List<LineageRelationship> oneHopRelationships =
          getLineageRelationshipsInBatches(currentLevel, direction, graphFilters, visitedEntities, i + 1, remainingTime);
      result.addAll(oneHopRelationships);
      currentLevel = oneHopRelationships.stream().map(LineageRelationship::getEntity).collect(Collectors.toList());
      currentTime = System.currentTimeMillis();
      remainingTime = timeoutTime - currentTime;
    }
    LineageResponse response = new LineageResponse(result.size(), result);

    List<LineageRelationship> subList;
    if (offset >= response.getTotal()) {
      subList = Collections.emptyList();
    } else {
      subList = response.getLineageRelationships().subList(offset, Math.min(offset + count, response.getTotal()));
    }

    return new LineageResponse(response.getTotal(), subList);
  }

  // Get 1-hop lineage relationships asynchronously in batches with timeout
  @WithSpan
  public List<LineageRelationship> getLineageRelationshipsInBatches(@Nonnull List<Urn> entityUrns,
      @Nonnull LineageDirection direction, GraphFilters graphFilters, Set<Urn> visitedEntities, int numHops, long remainingTime) {
    List<List<Urn>> batches = Lists.partition(entityUrns, BATCH_SIZE);
    return ConcurrencyUtils.getAllCompleted(batches.stream()
        .map(batchUrns -> CompletableFuture.supplyAsync(
            () -> getLineageRelationships(batchUrns, direction, graphFilters, visitedEntities, numHops)))
        .collect(Collectors.toList()), remainingTime, TimeUnit.MILLISECONDS)
        .stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  // Get 1-hop lineage relationships
  @WithSpan
  private List<LineageRelationship> getLineageRelationships(@Nonnull List<Urn> entityUrns,
      @Nonnull LineageDirection direction, GraphFilters graphFilters, Set<Urn> visitedEntities, int numHops) {
    Map<String, List<Urn>> urnsPerEntityType = entityUrns.stream().collect(Collectors.groupingBy(Urn::getEntityType));
    Map<String, List<EdgeInfo>> edgesPerEntityType = urnsPerEntityType.keySet()
        .stream()
        .collect(Collectors.toMap(Function.identity(),
            entityType -> lineageRegistry.getLineageRelationships(entityType, direction)));
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    // Get all relation types relevant to the set of urns to hop from
    urnsPerEntityType.forEach((entityType, urns) -> finalQuery.should(
        getQueryForLineage(urns, edgesPerEntityType.getOrDefault(entityType, Collections.emptyList()), graphFilters)));
    SearchResponse response = executeSearchQuery(finalQuery, 0, MAX_ELASTIC_RESULT);
    Set<Urn> entityUrnSet = new HashSet<>(entityUrns);
    // Get all valid edges given the set of urns to hop from
    Set<Pair<String, EdgeInfo>> validEdges = edgesPerEntityType.entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream().map(edgeInfo -> Pair.of(entry.getKey(), edgeInfo)))
        .collect(Collectors.toSet());
    return extractRelationships(entityUrnSet, response, validEdges, visitedEntities, numHops);
  }

  // Given set of edges and the search response, extract all valid edges that originate from the input entityUrns
  @WithSpan
  private List<LineageRelationship> extractRelationships(@Nonnull Set<Urn> entityUrns,
      @Nonnull SearchResponse searchResponse, Set<Pair<String, EdgeInfo>> validEdges, Set<Urn> visitedEntities,
      int numHops) {
    List<LineageRelationship> result = new LinkedList<>();
    for (SearchHit hit : searchResponse.getHits().getHits()) {
      Map<String, Object> document = hit.getSourceAsMap();
      Urn sourceUrn = UrnUtils.getUrn(((Map<String, Object>) document.get(SOURCE)).get("urn").toString());
      Urn destinationUrn =
          UrnUtils.getUrn(((Map<String, Object>) document.get(DESTINATION)).get("urn").toString());
      String type = document.get(RELATIONSHIP_TYPE).toString();

      // Potential outgoing edge
      if (entityUrns.contains(sourceUrn)) {
        // Skip if already visited
        // Skip if edge is not a valid outgoing edge
        if (!visitedEntities.contains(destinationUrn) && validEdges.contains(
            Pair.of(sourceUrn.getEntityType(), new EdgeInfo(type, RelationshipDirection.OUTGOING, destinationUrn.getEntityType().toLowerCase())))) {
          visitedEntities.add(destinationUrn);
          result.add(new LineageRelationship().setType(type).setEntity(destinationUrn).setDegree(numHops));
        }
      }

      // Potential incoming edge
      if (entityUrns.contains(destinationUrn)) {
        // Skip if already visited
        // Skip if edge is not a valid outgoing edge
        if (!visitedEntities.contains(sourceUrn) && validEdges.contains(
            Pair.of(destinationUrn.getEntityType(), new EdgeInfo(type, RelationshipDirection.INCOMING, sourceUrn.getEntityType().toLowerCase())))) {
          visitedEntities.add(sourceUrn);
          result.add(new LineageRelationship().setType(type).setEntity(sourceUrn).setDegree(numHops));
        }
      }
    }
    return result;
  }

  BoolQueryBuilder getOutGoingEdgeQuery(List<Urn> urns, List<EdgeInfo> outgoingEdges) {
    BoolQueryBuilder outgoingEdgeQuery = QueryBuilders.boolQuery();
    outgoingEdgeQuery.must(buildUrnFilters(urns, SOURCE));
    outgoingEdgeQuery.must(buildEdgeFilters(outgoingEdges));
    return outgoingEdgeQuery;
  }

  BoolQueryBuilder getIncomingEdgeQuery(List<Urn> urns, List<EdgeInfo> incomingEdges) {
    BoolQueryBuilder incomingEdgeQuery = QueryBuilders.boolQuery();
    incomingEdgeQuery.must(buildUrnFilters(urns, DESTINATION));
    incomingEdgeQuery.must(buildEdgeFilters(incomingEdges));
    return incomingEdgeQuery;
  }

  BoolQueryBuilder getAllowedEntityTypesFilter(GraphFilters graphFilters) {
    BoolQueryBuilder allowedEntityTypesFilter = QueryBuilders.boolQuery();
    allowedEntityTypesFilter.must(buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), SOURCE));
    allowedEntityTypesFilter.must(buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), DESTINATION));
    return allowedEntityTypesFilter;
  }

  // Get search query for given list of edges and source urns
  public QueryBuilder getQueryForLineage(List<Urn> urns, List<EdgeInfo> lineageEdges, GraphFilters graphFilters) {
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    if (lineageEdges.isEmpty()) {
      return query;
    }
    Map<RelationshipDirection, List<EdgeInfo>> edgesByDirection =
        lineageEdges.stream().collect(Collectors.groupingBy(EdgeInfo::getDirection));

    List<EdgeInfo> outgoingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.OUTGOING, Collections.emptyList());
    if (!outgoingEdges.isEmpty()) {
      query.should(getOutGoingEdgeQuery(urns, outgoingEdges));
    }

    List<EdgeInfo> incomingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.INCOMING, Collections.emptyList());
    if (!incomingEdges.isEmpty()) {
      query.should(getIncomingEdgeQuery(urns, incomingEdges));
    }

    if (graphFilters != null) {
      if (graphFilters.getAllowedEntityTypes() != null && !graphFilters.getAllowedEntityTypes().isEmpty())  {
        query.must(getAllowedEntityTypesFilter(graphFilters));
      }
    }
    return query;
  }

  public QueryBuilder buildEntityTypesFilter(List<String> entityTypes, String prefix) {
    return QueryBuilders.termsQuery(prefix + ".entityType", entityTypes.stream().map(Object::toString).collect(Collectors.toList()));
  }

  public QueryBuilder buildUrnFilters(List<Urn> urns, String prefix) {
    return QueryBuilders.termsQuery(prefix + ".urn", urns.stream().map(Object::toString).collect(Collectors.toList()));
  }

  public QueryBuilder buildEdgeFilters(List<EdgeInfo> edgeInfos) {
    return QueryBuilders.termsQuery("relationshipType",
        edgeInfos.stream().map(EdgeInfo::getType).distinct().collect(Collectors.toList()));
  }

  @Value
  public static class LineageResponse {
    int total;
    List<LineageRelationship> lineageRelationships;
  }
}
