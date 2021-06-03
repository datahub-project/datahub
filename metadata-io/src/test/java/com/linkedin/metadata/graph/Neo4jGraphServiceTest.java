package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static org.testng.Assert.*;


public class Neo4jGraphServiceTest {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jGraphService _client;

  @BeforeMethod
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());
    _client = new Neo4jGraphService(_driver);
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Test
  public void testAddEdge() throws Exception {
    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        "DownstreamOf");

    _client.addEdge(edge1);

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.OUTGOING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);
  }

  @Test
  public void testAddEdgeReverse() throws Exception {
    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "DownstreamOf");

    _client.addEdge(edge1);

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.INCOMING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);
  }

  @Test
  public void testRemoveEdgesFromNode() throws Exception {
    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "DownstreamOf");

    _client.addEdge(edge1);

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.INCOMING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);

    _client.removeEdgeTypesFromNode(Urn.createFromString(
        "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        edgeTypes,
        relationshipFilter);

    List<String> relatedUrnsPostDelete = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrnsPostDelete.size(), 0);
  }
}
