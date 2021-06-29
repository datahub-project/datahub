package com.linkedin.metadata.graph.elastic;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;


@Slf4j
@RequiredArgsConstructor
public class ElasticSearchGraphService implements GraphService {

  private static final int MAX_ELASTIC_RESULT = 10000;
  private final RestHighLevelClient searchClient;
  private final IndexConvention _indexConvention;
  private final ESGraphWriteDAO _graphWriteDAO;
  private final ESGraphQueryDAO _graphReadDAO;

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
      byte[] bytesOfRawDocID = rawDocId.getBytes("UTF-8");
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] thedigest = md.digest(bytesOfRawDocID);
      return thedigest.toString();
    } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
      e.printStackTrace();
      return rawDocId;
    }
  }

  public void addEdge(@Nonnull final Edge edge) {
    String docId = toDocId(edge);
    String edgeDocument = toDocument(edge);
    _graphWriteDAO.upsertDocument(docId, edgeDocument);
  }

  @Nonnull
  public List<String> findRelatedUrns(
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
      return ImmutableList.of();
    }

    return Arrays.stream(response.getHits().getHits())
        .map(hit -> ((HashMap<String, String>) hit.getSourceAsMap().getOrDefault(destinationNode, EMPTY_HASH)).getOrDefault("urn", null))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Filter createUrnFilter(@Nonnull final Urn urn) {
    Filter filter = new Filter();
    CriterionArray criterionArray = new CriterionArray();
    Criterion criterion = new Criterion();
    criterion.setCondition(Condition.EQUAL);
    criterion.setField("urn");
    criterion.setValue(urn.toString());
    criterionArray.add(criterion);
    filter.setCriteria(criterionArray);

    return filter;
  }

  public void removeNode(@Nonnull final Urn urn) {
    Filter urnFilter = createUrnFilter(urn);
    Filter emptyFilter = new Filter().setCriteria(new CriterionArray());
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
    Filter emptyFilter = new Filter().setCriteria(new CriterionArray());

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
    boolean exists = false;
    try {
      exists = searchClient.indices().exists(
          new GetIndexRequest(_indexConvention.getIndexName(INDEX_NAME)), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("ERROR: Failed to set up elasticsearch graph index. Could not check if the index exists");
      e.printStackTrace();
      return;
    }

    // If index doesn't exist, create index
    if (!exists) {
      log.info("Elastic Graph Index does not exist. Creating.");
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(_indexConvention.getIndexName(INDEX_NAME));

      createIndexRequest.mapping(GraphRelationshipMappingsBuilder.getMappings());
      createIndexRequest.settings(SettingsBuilder.getSettings());

      try {
        searchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        log.error("ERROR: Failed to set up elasticsearch graph index. Could not create the index.");
        e.printStackTrace();
        return;
      }

      log.info("Successfully Created Elastic Graph Index");
    }

    return;
  }

  @Override
  public void clear() {
    DeleteByQueryRequest deleteRequest =
        new DeleteByQueryRequest(_indexConvention.getIndexName(INDEX_NAME)).setQuery(QueryBuilders.matchAllQuery());
    try {
      searchClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Failed to clear graph service: {}", e.toString());
    }
  }
}
