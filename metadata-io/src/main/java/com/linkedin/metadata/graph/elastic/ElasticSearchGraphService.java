package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.*;
import static com.linkedin.metadata.graph.elastic.utils.GraphFilterUtils.getUrnStatusFieldName;
import static com.linkedin.metadata.graph.elastic.utils.GraphFilterUtils.getUrnStatusQuery;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
@RequiredArgsConstructor
public class ElasticSearchGraphService implements GraphService, ElasticSearchIndexed {
  private final LineageRegistry lineageRegistry;
  private final ESBulkProcessor esBulkProcessor;
  private final IndexConvention indexConvention;
  private final ESGraphWriteDAO graphWriteDAO;
  private final ESGraphQueryDAO graphReadDAO;
  private final ESIndexBuilder indexBuilder;
  private final String idHashAlgo;
  public static final String INDEX_NAME = "graph_service_v1";
  private static final Map<String, Object> EMPTY_HASH = new HashMap<>();

  private static String toDocument(@Nonnull final Edge edge) {
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();

    final ObjectNode sourceObject = JsonNodeFactory.instance.objectNode();
    sourceObject.put("urn", edge.getSource().toString());
    sourceObject.put("entityType", edge.getSource().getEntityType());

    final ObjectNode destinationObject = JsonNodeFactory.instance.objectNode();
    destinationObject.put("urn", edge.getDestination().toString());
    destinationObject.put("entityType", edge.getDestination().getEntityType());

    searchDocument.set(EDGE_FIELD_SOURCE, sourceObject);
    searchDocument.set(EDGE_FIELD_DESTINATION, destinationObject);
    searchDocument.put(EDGE_FIELD_RELNSHIP_TYPE, edge.getRelationshipType());
    if (edge.getCreatedOn() != null) {
      searchDocument.put("createdOn", edge.getCreatedOn());
    }
    if (edge.getCreatedActor() != null) {
      searchDocument.put("createdActor", edge.getCreatedActor().toString());
    }
    if (edge.getUpdatedOn() != null) {
      searchDocument.put("updatedOn", edge.getUpdatedOn());
    }
    if (edge.getUpdatedActor() != null) {
      searchDocument.put("updatedActor", edge.getUpdatedActor().toString());
    }
    if (edge.getProperties() != null) {
      final ObjectNode propertiesObject = JsonNodeFactory.instance.objectNode();
      for (Map.Entry<String, Object> entry : edge.getProperties().entrySet()) {
        if (entry.getValue() instanceof String) {
          propertiesObject.put(entry.getKey(), (String) entry.getValue());
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  "Tried setting properties on graph edge but property value type is not supported. Key: %s, Value: %s ",
                  entry.getKey(), entry.getValue()));
        }
      }
      searchDocument.set(EDGE_FIELD_PROPERTIES, propertiesObject);
    }
    if (edge.getLifecycleOwner() != null) {
      searchDocument.put(EDGE_FIELD_LIFECYCLE_OWNER, edge.getLifecycleOwner().toString());
    }
    if (edge.getVia() != null) {
      searchDocument.put(EDGE_FIELD_VIA, edge.getVia().toString());
    }
    if (edge.getViaStatus() != null) {
      searchDocument.put(EDGE_FIELD_VIA_STATUS, edge.getViaStatus());
    }
    if (edge.getLifecycleOwnerStatus() != null) {
      searchDocument.put(EDGE_FIELD_LIFECYCLE_OWNER_STATUS, edge.getLifecycleOwnerStatus());
    }
    if (edge.getSourceStatus() != null) {
      searchDocument.put(EDGE_SOURCE_STATUS, edge.getSourceStatus());
    }
    if (edge.getDestinationStatus() != null) {
      searchDocument.put(EDGE_DESTINATION_STATUS, edge.getDestinationStatus());
    }
    log.debug("Search doc for write {}", searchDocument);

    return searchDocument.toString();
  }

  @Override
  public GraphServiceConfiguration getGraphServiceConfig() {
    return graphReadDAO.getGraphServiceConfig();
  }

  public ElasticSearchConfiguration getESSearchConfig() {
    return graphReadDAO.getESSearchConfig();
  }

  @Override
  public ESIndexBuilder getIndexBuilder() {
    return indexBuilder;
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return lineageRegistry;
  }

  @Override
  public void addEdge(@Nonnull final Edge edge) {
    String docId = edge.toDocId(idHashAlgo);
    String edgeDocument = toDocument(edge);
    graphWriteDAO.upsertDocument(docId, edgeDocument);
  }

  @Override
  public void upsertEdge(@Nonnull final Edge edge) {
    addEdge(edge);
  }

  @Override
  public void removeEdge(@Nonnull final Edge edge) {
    String docId = edge.toDocId(idHashAlgo);
    graphWriteDAO.deleteDocument(docId);
  }

  @Override
  @Nonnull
  public RelatedEntitiesResult findRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      final int offset,
      @Nullable Integer count) {
    if (graphFilters.noResultsByType()) {
      return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
    }

    SearchResponse response =
        graphReadDAO.getSearchResponse(opContext, graphFilters, offset, count);

    if (response == null) {
      return new RelatedEntitiesResult(offset, 0, 0, ImmutableList.of());
    }

    int totalCount = (int) response.getHits().getTotalHits().value;
    final List<RelatedEntity> relationships =
        searchHitsToRelatedEntities(
                response.getHits().getHits(), graphFilters.getRelationshipDirection())
            .stream()
            .map(RelatedEntities::asRelatedEntity)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    return new RelatedEntitiesResult(offset, relationships.size(), totalCount, relationships);
  }

  @Nonnull
  @WithSpan
  @Override
  @Deprecated
  public EntityLineageResult getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    count = ConfigUtils.applyLimit(getGraphServiceConfig(), count);
    LineageResponse lineageResponse =
        graphReadDAO.getLineage(opContext, entityUrn, lineageGraphFilters, offset, count, maxHops);
    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray(lineageResponse.getLineageRelationships()))
        .setStart(offset)
        .setCount(count)
        .setTotal(lineageResponse.getTotal())
        .setPartial(lineageResponse.isPartial());
  }

  @Nonnull
  @WithSpan
  @Override
  public EntityLineageResult getImpactLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int maxHops) {
    LineageResponse lineageResponse =
        graphReadDAO.getImpactLineage(opContext, entityUrn, lineageGraphFilters, maxHops);
    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray(lineageResponse.getLineageRelationships()))
        .setStart(0)
        .setCount(lineageResponse.getLineageRelationships().size())
        .setTotal(lineageResponse.getTotal())
        .setPartial(lineageResponse.isPartial());
  }

  private static Filter createUrnFilter(@Nonnull final Urn urn) {
    Filter filter = new Filter();
    CriterionArray criterionArray = new CriterionArray();
    Criterion criterion = buildCriterion("urn", Condition.EQUAL, urn.toString());
    criterionArray.add(criterion);
    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(criterionArray))));

    return filter;
  }

  public void removeNode(@Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    Filter urnFilter = createUrnFilter(urn);

    graphWriteDAO.deleteByQuery(opContext, GraphFilters.outgoingFilter(urnFilter));
    graphWriteDAO.deleteByQuery(opContext, GraphFilters.incomingFilter(urnFilter));

    // Delete all edges where this entity is a lifecycle owner
    graphWriteDAO.deleteByQuery(opContext, GraphFilters.ALL, urn.toString());
  }

  @Override
  public void setEdgeStatus(
      @Nonnull Urn urn, boolean removed, @Nonnull EdgeUrnType... edgeUrnTypes) {

    for (EdgeUrnType edgeUrnType : edgeUrnTypes) {
      // Update the graph status fields per urn type which do not match target state
      QueryBuilder negativeQuery = getUrnStatusQuery(edgeUrnType, urn, !removed);

      // Set up the script to update the boolean field
      String scriptContent =
          "ctx._source." + getUrnStatusFieldName(edgeUrnType) + " = params.newValue";
      Script script =
          new Script(
              ScriptType.INLINE,
              "painless",
              scriptContent,
              Collections.singletonMap("newValue", removed));

      graphWriteDAO.updateByQuery(script, negativeQuery);
    }
  }

  public void removeEdgesFromNode(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {

    graphWriteDAO.deleteByQuery(
        opContext, GraphFilters.from(createUrnFilter(urn), relationshipTypes, relationshipFilter));
  }

  @Override
  public void reindexAll(
      @Nonnull final OperationContext opContext,
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    log.info("Setting up elastic graph index");
    try {
      for (ReindexConfig config : buildReindexConfigs(opContext, properties)) {
        indexBuilder.buildIndex(config);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      @Nonnull final OperationContext opContext,
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties)
      throws IOException {
    return List.of(
        indexBuilder.buildReindexState(
            indexConvention.getIndexName(INDEX_NAME),
            GraphRelationshipMappingsBuilder.getMappings(),
            Collections.emptyMap()));
  }

  @Override
  public void clear() {
    // Instead of deleting all documents (inefficient), delete and recreate the index
    String indexName = indexConvention.getIndexName(INDEX_NAME);
    try {
      // Build a config with the correct target mappings for recreation
      ReindexConfig config =
          indexBuilder.buildReindexState(
              indexName, GraphRelationshipMappingsBuilder.getMappings(), Collections.emptyMap());

      // Use clearIndex which handles deletion and recreation
      indexBuilder.clearIndex(indexName, config);

      log.info("Cleared index {} by deleting and recreating it", indexName);
    } catch (IOException e) {
      log.error("Failed to clear index {}", indexName, e);
      throw new RuntimeException("Failed to clear index: " + indexName, e);
    }
  }

  @Override
  public boolean supportsMultiHop() {
    return true;
  }

  @Nonnull
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {

    count = ConfigUtils.applyLimit(getGraphServiceConfig(), count);
    SearchResponse response =
        graphReadDAO.getSearchResponse(
            opContext, graphFilters, sortCriteria, scrollId, keepAlive, count);

    if (response == null) {
      return new RelatedEntitiesScrollResult(0, 0, null, ImmutableList.of());
    }

    int totalCount = (int) response.getHits().getTotalHits().value;
    final List<RelatedEntities> relationships =
        searchHitsToRelatedEntities(
            response.getHits().getHits(), graphFilters.getRelationshipDirection());

    SearchHit[] searchHits = response.getHits().getHits();
    String pitId = response.pointInTimeId();
    if (pitId != null && keepAlive == null) {
      throw new IllegalArgumentException("Should not set pitId without keepAlive");
    }
    long expirationTime =
        keepAlive == null
            ? 0L
            : System.currentTimeMillis()
                + TimeValue.parseTimeValue(keepAlive, "keepAlive").millis();
    String nextScrollId = SearchAfterWrapper.nextScrollId(searchHits, count, pitId, expirationTime);
    if (nextScrollId == null && pitId != null) {
      // Last scroll, we clean up the pitId assuming user has gone through all data
      graphReadDAO.cleanupPointInTime(pitId);
    }

    return RelatedEntitiesScrollResult.builder()
        .entities(relationships)
        .pageSize(relationships.size())
        .numResults(totalCount)
        .scrollId(nextScrollId)
        .build();
  }

  /**
   * Returns list of edge documents for the given graph node and relationship tuples. Non-directed
   *
   * @param opContext operation context
   * @param edgeTuples Non-directed nodes and relationship types
   * @return list of documents matching the input criteria
   */
  @Override
  public List<Map<String, Object>> raw(OperationContext opContext, List<EdgeTuple> edgeTuples) {

    if (edgeTuples == null || edgeTuples.isEmpty()) {
      return Collections.emptyList();
    }

    List<Map<String, Object>> results = new ArrayList<>();

    // Build a single query for all edge tuples
    BoolQueryBuilder mainQuery = QueryBuilders.boolQuery();

    // For each edge tuple, create a query that matches edges in either direction
    for (EdgeTuple tuple : edgeTuples) {
      if (tuple.getA() == null || tuple.getB() == null || tuple.getRelationshipType() == null) {
        continue;
      }

      // Create a query for this specific edge tuple (non-directed)
      BoolQueryBuilder tupleQuery = QueryBuilders.boolQuery();

      // Match relationship type
      tupleQuery.filter(
          QueryBuilders.termQuery(EDGE_FIELD_RELNSHIP_TYPE, tuple.getRelationshipType()));

      // Match nodes in either direction: (a->b) OR (b->a)
      BoolQueryBuilder directionQuery = QueryBuilders.boolQuery();

      // Direction 1: a is source, b is destination
      BoolQueryBuilder direction1 = QueryBuilders.boolQuery();
      direction1.filter(QueryBuilders.termQuery(EDGE_FIELD_SOURCE + ".urn", tuple.getA()));
      direction1.filter(QueryBuilders.termQuery(EDGE_FIELD_DESTINATION + ".urn", tuple.getB()));

      // Direction 2: b is source, a is destination
      BoolQueryBuilder direction2 = QueryBuilders.boolQuery();
      direction2.filter(QueryBuilders.termQuery(EDGE_FIELD_SOURCE + ".urn", tuple.getB()));
      direction2.filter(QueryBuilders.termQuery(EDGE_FIELD_DESTINATION + ".urn", tuple.getA()));

      // Either direction is acceptable
      directionQuery.should(direction1);
      directionQuery.should(direction2);
      directionQuery.minimumShouldMatch(1);

      // Combine relationship type and direction queries
      tupleQuery.filter(directionQuery);

      // Add this tuple query as a "should" clause (OR condition)
      mainQuery.should(tupleQuery);
    }

    // At least one of the edge tuples must match
    mainQuery.minimumShouldMatch(1);

    // Build search request
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.query(mainQuery);
    // Set a reasonable size limit - adjust based on expected number of edges
    searchSourceBuilder.size(getGraphServiceConfig().getLimit().getResults().getApiDefault());

    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    // Execute search using the graphReadDAO's search client
    SearchResponse searchResponse = graphReadDAO.executeSearch(searchRequest);
    SearchHits hits = searchResponse.getHits();

    // Process each hit
    for (SearchHit hit : hits.getHits()) {
      Map<String, Object> sourceMap = hit.getSourceAsMap();

      results.add(sourceMap);
    }

    // Log if we hit the size limit
    if (hits.getTotalHits() != null && hits.getTotalHits().value > hits.getHits().length) {
      log.warn(
          "Total hits {} exceeds returned size {}. Some edges may be missing. "
              + "Consider implementing pagination or increasing the size limit.",
          hits.getTotalHits().value,
          hits.getHits().length);
    }

    return results;
  }

  private static List<RelatedEntities> searchHitsToRelatedEntities(
      SearchHit[] searchHits, RelationshipDirection relationshipDirection) {
    return Arrays.stream(searchHits)
        .map(
            hit -> {
              final Map<String, Object> hitMap = hit.getSourceAsMap();
              final String destinationUrnStr =
                  ((Map<String, String>) hitMap.getOrDefault(EDGE_FIELD_DESTINATION, EMPTY_HASH))
                      .getOrDefault("urn", null);
              final String sourceUrnStr =
                  ((Map<String, String>) hitMap.getOrDefault(EDGE_FIELD_SOURCE, EMPTY_HASH))
                      .getOrDefault("urn", null);
              final String relationshipType = (String) hitMap.get(EDGE_FIELD_RELNSHIP_TYPE);
              String viaEntity = (String) hitMap.get(EDGE_FIELD_VIA);

              if (destinationUrnStr == null || sourceUrnStr == null || relationshipType == null) {
                log.error(
                    String.format(
                        "Found null urn string, relationship type, aspect name or path spec in Elastic index. "
                            + "destinationUrnStr: %s, sourceUrnStr: %s, relationshipType: %s",
                        destinationUrnStr, sourceUrnStr, relationshipType));
                return null;
              }

              return new RelatedEntities(
                  relationshipType,
                  sourceUrnStr,
                  destinationUrnStr,
                  relationshipDirection,
                  viaEntity);
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
