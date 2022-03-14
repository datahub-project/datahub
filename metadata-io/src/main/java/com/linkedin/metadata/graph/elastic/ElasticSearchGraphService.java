package com.linkedin.metadata.graph.elastic;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;


@Slf4j
@RequiredArgsConstructor
public class ElasticSearchGraphService implements GraphService {

  private final LineageRegistry _lineageRegistry;
  private final RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention;
  private final ESGraphWriteDAO _graphWriteDAO;
  private final ESGraphQueryDAO _graphReadDAO;
  private final ESIndexBuilder _indexBuilder;

  private static final String DOC_DELIMETER = "--";
  public static final String INDEX_NAME = "graph_service_v1";
  private static final Map<String, Object> EMPTY_HASH = new HashMap<>();

  private String toDocument(@Nonnull final Edge edge) {
    final ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();

    final ObjectNode sourceObject = JsonNodeFactory.instance.objectNode();
    sourceObject.put("urn", edge.getSource().toString());
    sourceObject.put("entityType", edge.getSource().getEntityType());

    final ObjectNode destinationObject = JsonNodeFactory.instance.objectNode();
    destinationObject.put("urn", edge.getDestination().toString());
    destinationObject.put("entityType", edge.getDestination().getEntityType());

    searchDocument.set("source", sourceObject);
    searchDocument.set("destination", destinationObject);
    searchDocument.put("relationshipType", edge.getRelationshipType());

    return searchDocument.toString();
  }

  private String toDocId(@Nonnull final Edge edge) {
    String rawDocId =
        edge.getSource().toString() + DOC_DELIMETER + edge.getRelationshipType() + DOC_DELIMETER + edge.getDestination().toString();

    try {
      byte[] bytesOfRawDocID = rawDocId.getBytes(StandardCharsets.UTF_8);
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] thedigest = md.digest(bytesOfRawDocID);
      return Base64.getEncoder().encodeToString(thedigest);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return rawDocId;
    }
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return _lineageRegistry;
  }

  public void addEdge(@Nonnull final Edge edge) {
    String docId = toDocId(edge);
    String edgeDocument = toDocument(edge);
    _graphWriteDAO.upsertDocument(docId, edgeDocument);
  }

  @Nonnull
  public RelatedEntitiesResult findRelatedEntities(
      @Nullable final String sourceType,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      final int offset,
      final int count) {

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();
    String destinationNode = relationshipDirection == RelationshipDirection.OUTGOING ? "destination" : "source";

    SearchResponse response = _graphReadDAO.getSearchResponse(
        sourceType,
        sourceEntityFilter,
        destinationType,
        destinationEntityFilter,
        relationshipTypes,
        relationshipFilter,
        offset,
        count
    );

    if (response == null) {
      return new RelatedEntitiesResult(offset, 0, 0, ImmutableList.of());
    }

    int totalCount = (int) response.getHits().getTotalHits().value;
    final List<RelatedEntity> relationships = Arrays.stream(response.getHits().getHits())
        .map(hit -> {
          final String urnStr = ((HashMap<String, String>) hit.getSourceAsMap().getOrDefault(destinationNode, EMPTY_HASH)).getOrDefault("urn", null);
          final String relationshipType = (String) hit.getSourceAsMap().get("relationshipType");
          if (urnStr == null || relationshipType == null) {
            log.error(String.format(
                "Found null urn string or relationship type in Elastic index. urnStr: %s, relationshipType: %s", urnStr, relationshipType));
            return null;
          }
          return new RelatedEntity(relationshipType, urnStr);
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    return new RelatedEntitiesResult(offset, relationships.size(), totalCount, relationships);
  }

  @Nonnull
  @WithSpan
  @Override
  public EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction, int offset,
      int count, int maxHops) {
    ESGraphQueryDAO.LineageResponse lineageResponse =
        _graphReadDAO.getLineage(entityUrn, direction, offset, count, maxHops);
    return new EntityLineageResult().setRelationships(
        new LineageRelationshipArray(lineageResponse.getLineageRelationships()))
        .setStart(offset)
        .setCount(count)
        .setTotal(lineageResponse.getTotal());
  }

  private Filter createUrnFilter(@Nonnull final Urn urn) {
    Filter filter = new Filter();
    CriterionArray criterionArray = new CriterionArray();
    Criterion criterion = new Criterion();
    criterion.setCondition(Condition.EQUAL);
    criterion.setField("urn");
    criterion.setValue(urn.toString());
    criterionArray.add(criterion);
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(criterionArray))));

    return filter;
  }

  public void removeNode(@Nonnull final Urn urn) {
    Filter urnFilter = createUrnFilter(urn);
    Filter emptyFilter = new Filter().setOr(new ConjunctiveCriterionArray());
    List<String> relationshipTypes = new ArrayList<>();

    RelationshipFilter outgoingFilter = new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);
    RelationshipFilter incomingFilter = new RelationshipFilter().setDirection(RelationshipDirection.INCOMING);

    _graphWriteDAO.deleteByQuery(
        null,
        urnFilter,
        null,
        emptyFilter,
        relationshipTypes,
        outgoingFilter
    );

    _graphWriteDAO.deleteByQuery(
        null,
        urnFilter,
        null,
        emptyFilter,
        relationshipTypes,
        incomingFilter
    );

    return;
  }

  public void removeEdgesFromNode(
      @Nonnull final Urn urn,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {

    Filter urnFilter = createUrnFilter(urn);
    Filter emptyFilter = new Filter().setOr(new ConjunctiveCriterionArray());

    _graphWriteDAO.deleteByQuery(
        null,
        urnFilter,
        null,
        emptyFilter,
        relationshipTypes,
        relationshipFilter
    );
  }

  @Override
  public void configure() {
    log.info("Setting up elastic graph index");
    try {
      _indexBuilder.buildIndex(_indexConvention.getIndexName(INDEX_NAME),
          GraphRelationshipMappingsBuilder.getMappings(), Collections.emptyMap());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void clear() {
    DeleteByQueryRequest deleteRequest =
        new DeleteByQueryRequest(_indexConvention.getIndexName(INDEX_NAME)).setQuery(QueryBuilders.matchAllQuery());
    try {
      _searchClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Failed to clear graph service: {}", e.toString());
    }
  }

  @Override
  public boolean supportsMultiHop() {
    return true;
  }
}
