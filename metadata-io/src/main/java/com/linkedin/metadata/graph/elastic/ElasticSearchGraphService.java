package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.*;
import static com.linkedin.metadata.graph.elastic.GraphFilterUtils.getUrnStatusFieldName;
import static com.linkedin.metadata.graph.elastic.GraphFilterUtils.getUrnStatusQuery;
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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;

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
    return graphReadDAO.getConfig();
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
    ESGraphQueryDAO.LineageResponse lineageResponse =
        graphReadDAO.getLineage(opContext, entityUrn, lineageGraphFilters, offset, count, maxHops);
    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray(lineageResponse.getLineageRelationships()))
        .setStart(offset)
        .setCount(count)
        .setTotal(lineageResponse.getTotal());
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
  public void reindexAll(Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    log.info("Setting up elastic graph index");
    try {
      for (ReindexConfig config : buildReindexConfigs(properties)) {
        indexBuilder.buildIndex(config);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) throws IOException {
    return List.of(
        indexBuilder.buildReindexState(
            indexConvention.getIndexName(INDEX_NAME),
            GraphRelationshipMappingsBuilder.getMappings(),
            Collections.emptyMap()));
  }

  @Override
  public void clear() {
    esBulkProcessor.deleteByQuery(
        QueryBuilders.matchAllQuery(), true, indexConvention.getIndexName(INDEX_NAME));
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
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {

    count = ConfigUtils.applyLimit(getGraphServiceConfig(), count);
    SearchResponse response =
        graphReadDAO.getSearchResponse(opContext, graphFilters, sortCriteria, scrollId, count);

    if (response == null) {
      return new RelatedEntitiesScrollResult(0, 0, null, ImmutableList.of());
    }

    int totalCount = (int) response.getHits().getTotalHits().value;
    final List<RelatedEntities> relationships =
        searchHitsToRelatedEntities(
            response.getHits().getHits(), graphFilters.getRelationshipDirection());

    SearchHit[] searchHits = response.getHits().getHits();
    // Only return next scroll ID if there are more results, indicated by full size results
    String nextScrollId = null;
    if (searchHits.length == count) {
      Object[] sort = searchHits[searchHits.length - 1].getSortValues();
      nextScrollId = new SearchAfterWrapper(sort, null, 0L).toScrollId();
    }

    return RelatedEntitiesScrollResult.builder()
        .entities(relationships)
        .pageSize(relationships.size())
        .numResults(totalCount)
        .scrollId(nextScrollId)
        .build();
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
