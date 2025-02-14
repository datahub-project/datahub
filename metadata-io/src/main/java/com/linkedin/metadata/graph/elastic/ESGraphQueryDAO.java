package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.*;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.*;

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
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageDirection;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortOrder;

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
  static final String SCORE_FIELD = "_score";
  static final String UI = "UI";
  static final String FILTER_BY_SOURCE_RELATIONSHIP = "filter_by_source_relationship";
  static final String FILTER_BY_DESTINATION_RELATIONSHIP = "filter_by_destination_relationship";
  static final String GROUP_BY_SOURCE_AGG = "group_by_source";
  static final String GROUP_BY_DESTINATION_AGG = "group_by_destination";
  static final String TOP_DOCUMENTS_AGG = "top_documents";

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
    searchSourceBuilder.size(count);

    searchSourceBuilder.query(query);
    if (graphQueryConfiguration.isBoostViaNodes()) {
      addViaNodeBoostQuery(searchSourceBuilder);
    }
    return searchSourceBuilder;
  }

  private SearchResponse executeGroupByLineageSearchQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final QueryBuilder query,
      final int offset,
      final int count,
      final Set<Pair<String, EdgeInfo>> validEdges) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = sharedSourceBuilder(query, offset, 0);

    // We have to group by both Source AND Destination because edge types may go in different
    // directions for lineage
    // set up filters for each relationship type in the correct direction to limit buckets
    BoolQueryBuilder sourceFilterQuery = QueryBuilders.boolQuery();

    validEdges.stream()
        .filter(pair -> RelationshipDirection.OUTGOING.equals(pair.getValue().getDirection()))
        .forEach(
            pair ->
                sourceFilterQuery.should(
                    getAggregationFilter(pair, RelationshipDirection.OUTGOING)));
    if (!sourceFilterQuery.should().isEmpty()) {
      sourceFilterQuery.minimumShouldMatch(1);
    }

    BoolQueryBuilder destFilterQuery = QueryBuilders.boolQuery();
    validEdges.stream()
        .filter(pair -> RelationshipDirection.INCOMING.equals(pair.getValue().getDirection()))
        .forEach(
            pair ->
                destFilterQuery.should(getAggregationFilter(pair, RelationshipDirection.INCOMING)));
    if (!destFilterQuery.should().isEmpty()) {
      destFilterQuery.minimumShouldMatch(1);
    }

    FilterAggregationBuilder sourceRelationshipTypeFilters =
        AggregationBuilders.filter(FILTER_BY_SOURCE_RELATIONSHIP, sourceFilterQuery);
    FilterAggregationBuilder destRelationshipTypeFilters =
        AggregationBuilders.filter(FILTER_BY_DESTINATION_RELATIONSHIP, destFilterQuery);
    TermsAggregationBuilder sourceAgg =
        AggregationBuilders.terms(GROUP_BY_SOURCE_AGG)
            .field(SOURCE + ".urn")
            .size(
                graphQueryConfiguration
                    .getBatchSize()); // Number of buckets can be up to batch size per query for
    // each

    TermsAggregationBuilder destAgg =
        AggregationBuilders.terms(GROUP_BY_DESTINATION_AGG)
            .field(DESTINATION + ".urn")
            .size(graphQueryConfiguration.getBatchSize());

    TopHitsAggregationBuilder topHitsAgg =
        AggregationBuilders.topHits(TOP_DOCUMENTS_AGG)
            .size(count)
            .sort(SCORE_FIELD, SortOrder.DESC);
    sourceAgg.subAggregation(topHitsAgg);
    destAgg.subAggregation(topHitsAgg);

    sourceRelationshipTypeFilters.subAggregation(sourceAgg);
    destRelationshipTypeFilters.subAggregation(destAgg);
    searchSourceBuilder.aggregation(sourceRelationshipTypeFilters);
    searchSourceBuilder.aggregation(destRelationshipTypeFilters);
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    return opContext.withSpan(
        "esLineageGroupByQuery",
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
        MetricUtils.name(this.getClass(), "esLineageGroupByQuery"));
  }

  private static BoolQueryBuilder getAggregationFilter(
      Pair<String, EdgeInfo> pair, RelationshipDirection direction) {
    BoolQueryBuilder subFilter = QueryBuilders.boolQuery();
    TermQueryBuilder relationshipTypeTerm =
        QueryBuilders.termQuery(RELATIONSHIP_TYPE, pair.getValue().getType()).caseInsensitive(true);
    subFilter.must(relationshipTypeTerm);

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
        QueryBuilders.termQuery(SOURCE + ".entityType", sourceType).caseInsensitive(true);
    subFilter.must(sourceTypeTerm);
    TermQueryBuilder destinationTypeTerm =
        QueryBuilders.termQuery(DESTINATION + ".entityType", destinationType).caseInsensitive(true);
    subFilter.must(destinationTypeTerm);
    return subFilter;
  }

  public SearchResponse getSearchResponse(
      @Nonnull final OperationContext opContext,
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
            opContext,
            graphQueryConfiguration,
            sourceTypes,
            sourceEntityFilter,
            destinationTypes,
            destinationEntityFilter,
            relationshipTypes,
            relationshipFilter);

    return executeLineageSearchQuery(opContext, finalQuery, offset, count);
  }

  public static BoolQueryBuilder buildQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphQueryConfiguration graphQueryConfiguration,
      @Nullable final List<String> sourceTypes,
      @Nullable final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes,
      @Nullable final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {
    return buildQuery(
        opContext,
        graphQueryConfiguration,
        sourceTypes,
        sourceEntityFilter,
        destinationTypes,
        destinationEntityFilter,
        relationshipTypes,
        relationshipFilter,
        null);
  }

  public static BoolQueryBuilder buildQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphQueryConfiguration graphQueryConfiguration,
      @Nullable final List<String> sourceTypes,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      @Nullable final String lifecycleOwner) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    // set source filter
    String sourceNode =
        relationshipDirection == RelationshipDirection.OUTGOING ? SOURCE : DESTINATION;
    if (sourceTypes != null && sourceTypes.size() > 0) {
      finalQuery.filter(QueryBuilders.termsQuery(sourceNode + ".entityType", sourceTypes));
    }
    if (sourceEntityFilter != null) {
      addFilterToQueryBuilder(sourceEntityFilter, sourceNode, finalQuery);
    }

    // set destination filter
    String destinationNode =
        relationshipDirection == RelationshipDirection.OUTGOING ? DESTINATION : SOURCE;
    if (destinationTypes != null && destinationTypes.size() > 0) {
      finalQuery.filter(
          QueryBuilders.termsQuery(destinationNode + ".entityType", destinationTypes));
    }
    if (destinationEntityFilter != null) {
      addFilterToQueryBuilder(destinationEntityFilter, destinationNode, finalQuery);
    }

    // set relationship filter
    if (relationshipTypes.size() > 0) {
      BoolQueryBuilder relationshipQuery = QueryBuilders.boolQuery();
      relationshipTypes.forEach(
          relationshipType ->
              relationshipQuery.should(
                  QueryBuilders.termQuery(RELATIONSHIP_TYPE, relationshipType)));
      if (!relationshipQuery.should().isEmpty()) {
        relationshipQuery.minimumShouldMatch(1);
      }
      finalQuery.filter(relationshipQuery);
    }

    // general filter
    if (relationshipFilter.getOr() != null) {
      addFilterToQueryBuilder(new Filter().setOr(relationshipFilter.getOr()), null, finalQuery);
    }
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
      @Nonnull LineageDirection direction,
      GraphFilters graphFilters,
      int offset,
      int count,
      int maxHops) {
    Map<Urn, LineageRelationship> result = new HashMap<>();
    long currentTime = System.currentTimeMillis();
    long remainingTime = graphQueryConfiguration.getTimeoutSeconds() * 1000;
    boolean exploreMultiplePaths = graphQueryConfiguration.isEnableMultiPathSearch();
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
            direction,
            maxHops);
        break;
      }

      // Do one hop on the lineage graph
      Stream<Urn> intermediateStream =
          processOneHopLineage(
              opContext,
              currentLevel,
              remainingTime,
              direction,
              maxHops,
              graphFilters,
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
      LineageDirection direction,
      int maxHops,
      GraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      Map<Urn, UrnArrayArray> existingPaths,
      boolean exploreMultiplePaths,
      Map<Urn, LineageRelationship> result,
      int i) {

    // Do one hop on the lineage graph
    int numHops = i + 1; // Zero indexed for loop counter, one indexed count
    int remainingHops = maxHops - numHops;
    List<LineageRelationship> oneHopRelationships =
        getLineageRelationshipsInBatches(
            opContext,
            currentLevel,
            direction,
            graphFilters,
            visitedEntities,
            viaEntities,
            numHops,
            remainingHops,
            remainingTime,
            existingPaths,
            exploreMultiplePaths);

    final LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();

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
                  direction,
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
      @Nonnull LineageDirection direction,
      GraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      long remainingTime,
      Map<Urn, UrnArrayArray> existingPaths,
      boolean exploreMultiplePaths) {
    List<List<Urn>> batches = Lists.partition(entityUrns, graphQueryConfiguration.getBatchSize());
    return ConcurrencyUtils.getAllCompleted(
            batches.stream()
                .map(
                    batchUrns ->
                        CompletableFuture.supplyAsync(
                            () ->
                                getLineageRelationships(
                                    opContext,
                                    batchUrns,
                                    direction,
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
      @Nonnull LineageDirection direction,
      GraphFilters graphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      Map<Urn, UrnArrayArray> existingPaths,
      boolean exploreMultiplePaths) {
    final LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();
    Map<String, List<Urn>> urnsPerEntityType =
        entityUrns.stream().collect(Collectors.groupingBy(Urn::getEntityType));
    Map<String, List<EdgeInfo>> edgesPerEntityType =
        urnsPerEntityType.keySet().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    entityType -> lineageRegistry.getLineageRelationships(entityType, direction)));
    Set<Urn> entityUrnSet = new HashSet<>(entityUrns);
    // Get all valid edges given the set of urns to hop from
    Set<Pair<String, EdgeInfo>> validEdges =
        edgesPerEntityType.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().stream().map(edgeInfo -> Pair.of(entry.getKey(), edgeInfo)))
            .collect(Collectors.toSet());

    QueryBuilder finalQuery =
        getLineageQuery(opContext, urnsPerEntityType, edgesPerEntityType, graphFilters);
    SearchResponse response;
    if (lineageFlags != null && lineageFlags.getEntitiesExploredPerHopLimit() != null) {
      response =
          executeGroupByLineageSearchQuery(
              opContext, finalQuery, 0, lineageFlags.getEntitiesExploredPerHopLimit(), validEdges);
      return extractRelationshipsGroupByQuery(
          entityUrnSet,
          response,
          validEdges,
          visitedEntities,
          viaEntities,
          numHops,
          remainingHops,
          existingPaths,
          exploreMultiplePaths);
    } else {
      response =
          executeLineageSearchQuery(
              opContext, finalQuery, 0, graphQueryConfiguration.getMaxResult());
      return extractRelationships(
          entityUrnSet,
          response,
          validEdges,
          visitedEntities,
          viaEntities,
          numHops,
          remainingHops,
          existingPaths,
          exploreMultiplePaths);
    }
  }

  @VisibleForTesting
  public static QueryBuilder getLineageQuery(
      @Nonnull OperationContext opContext,
      @Nonnull Map<String, List<Urn>> urnsPerEntityType,
      @Nonnull Map<String, List<EdgeInfo>> edgesPerEntityType,
      @Nonnull GraphFilters graphFilters) {
    final LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();
    BoolQueryBuilder entityTypeQueries = QueryBuilders.boolQuery();
    // Get all relation types relevant to the set of urns to hop from
    urnsPerEntityType.forEach(
        (entityType, urns) -> {
          if (edgesPerEntityType.containsKey(entityType)
              && !edgesPerEntityType.get(entityType).isEmpty()) {
            entityTypeQueries.should(
                getLineageQueryForEntityType(
                    urns, edgesPerEntityType.get(entityType), graphFilters));
          }
        });
    if (!entityTypeQueries.should().isEmpty()) {
      entityTypeQueries.minimumShouldMatch(1);
    }

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    finalQuery.filter(entityTypeQueries);
    finalQuery.filter(buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), SOURCE));
    finalQuery.filter(buildEntityTypesFilter(graphFilters.getAllowedEntityTypes(), DESTINATION));

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
      @Nonnull List<Urn> urns,
      @Nonnull List<EdgeInfo> lineageEdges,
      @Nonnull GraphFilters graphFilters) {
    BoolQueryBuilder query = QueryBuilders.boolQuery();
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

    if (!query.should().isEmpty()) {
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
        graphQueryConfiguration.getMaxResult()); // Will rescore all results
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
      Set<Pair<String, EdgeInfo>> validEdges,
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
            validEdges,
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
      Set<Pair<String, EdgeInfo>> validEdges,
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
          validEdges,
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
          validEdges,
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
      Set<Pair<String, EdgeInfo>> validEdges,
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
          && validEdges.contains(
              Pair.of(
                  sourceUrn.getEntityType(),
                  new EdgeInfo(
                      type,
                      RelationshipDirection.OUTGOING,
                      destinationUrn.getEntityType().toLowerCase())))) {

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
      Set<Pair<String, EdgeInfo>> validEdges,
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
          && validEdges.contains(
              Pair.of(
                  destinationUrn.getEntityType(),
                  new EdgeInfo(
                      type,
                      RelationshipDirection.INCOMING,
                      sourceUrn.getEntityType().toLowerCase())))) {
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

  @WithSpan
  private static List<LineageRelationship> extractRelationshipsGroupByQuery(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull SearchResponse searchResponse,
      Set<Pair<String, EdgeInfo>> validEdges,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      Map<Urn, UrnArrayArray> existingPaths,
      boolean exploreMultiplePaths) {
    try {
      Map<Urn, LineageRelationship> lineageRelationshipMap = new HashMap<>();
      ParsedFilter sourceFilterAgg =
          searchResponse.getAggregations().get(FILTER_BY_SOURCE_RELATIONSHIP);
      ParsedStringTerms sourceTermsAgg = sourceFilterAgg.getAggregations().get(GROUP_BY_SOURCE_AGG);
      SearchHit[] hits = new SearchHit[0];
      List<? extends ParsedStringTerms.ParsedBucket> sourceBuckets =
          (List<? extends ParsedStringTerms.ParsedBucket>) sourceTermsAgg.getBuckets();
      int index = -1;
      for (ParsedStringTerms.ParsedBucket bucket : sourceBuckets) {
        ParsedTopHits topHits = bucket.getAggregations().get(TOP_DOCUMENTS_AGG);
        SearchHit[] topHitsArray = topHits.getHits().getHits();
        boolean truncatedChildren = topHits.getHits().getTotalHits().value > topHitsArray.length;
        for (SearchHit hit : topHitsArray) {
          processSearchHit(
              hit,
              entityUrns,
              index,
              exploreMultiplePaths,
              visitedEntities,
              validEdges,
              existingPaths,
              numHops,
              truncatedChildren,
              lineageRelationshipMap,
              viaEntities);
        }
      }

      ParsedFilter destFilterAgg =
          searchResponse.getAggregations().get(FILTER_BY_DESTINATION_RELATIONSHIP);
      ParsedStringTerms destTermsAgg =
          destFilterAgg.getAggregations().get(GROUP_BY_DESTINATION_AGG);
      List<? extends ParsedStringTerms.ParsedBucket> destBuckets =
          (List<? extends ParsedStringTerms.ParsedBucket>) destTermsAgg.getBuckets();
      for (ParsedStringTerms.ParsedBucket bucket : destBuckets) {
        ParsedTopHits topHits = bucket.getAggregations().get(TOP_DOCUMENTS_AGG);
        SearchHit[] topHitsArray = topHits.getHits().getHits();
        boolean truncatedChildren = topHits.getHits().getTotalHits().value > topHitsArray.length;
        for (SearchHit hit : topHitsArray) {
          processSearchHit(
              hit,
              entityUrns,
              index,
              exploreMultiplePaths,
              visitedEntities,
              validEdges,
              existingPaths,
              numHops,
              truncatedChildren,
              lineageRelationshipMap,
              viaEntities);
        }
      }
      log.debug("numHits: {}, numHops {}, remainingHops {}", hits.length, numHops, remainingHops);

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

  private static BoolQueryBuilder getOutGoingEdgeQuery(
      @Nonnull List<Urn> urns,
      @Nonnull List<EdgeInfo> outgoingEdges,
      @Nonnull GraphFilters graphFilters) {
    BoolQueryBuilder outgoingEdgeQuery = QueryBuilders.boolQuery();
    outgoingEdgeQuery.must(buildUrnFilters(urns, SOURCE));
    outgoingEdgeQuery.must(buildEdgeFilters(outgoingEdges));
    return outgoingEdgeQuery;
  }

  private static BoolQueryBuilder getIncomingEdgeQuery(
      @Nonnull List<Urn> urns, List<EdgeInfo> incomingEdges, @Nonnull GraphFilters graphFilters) {
    BoolQueryBuilder incomingEdgeQuery = QueryBuilders.boolQuery();
    incomingEdgeQuery.must(buildUrnFilters(urns, DESTINATION));
    incomingEdgeQuery.must(buildEdgeFilters(incomingEdges));
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
    // dedup urns while preserving order
    LinkedHashSet<String> urnSet = new LinkedHashSet<>();
    urns.forEach(urn -> urnSet.add(urn.toString()));
    return QueryBuilders.termsQuery(prefix + ".urn", urnSet);
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

  public SearchResponse getSearchResponse(
      @Nonnull final OperationContext opContext,
      @Nullable final List<String> sourceTypes,
      @Nullable final Filter sourceEntityFilter,
      @Nullable final List<String> destinationTypes,
      @Nullable final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      int count) {

    BoolQueryBuilder finalQuery =
        buildQuery(
            opContext,
            graphQueryConfiguration,
            sourceTypes,
            sourceEntityFilter,
            destinationTypes,
            destinationEntityFilter,
            relationshipTypes,
            relationshipFilter);

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

    searchSourceBuilder.size(count);
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
}
