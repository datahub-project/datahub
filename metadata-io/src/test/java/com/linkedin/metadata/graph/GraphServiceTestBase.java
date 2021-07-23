package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static org.testng.Assert.assertEquals;


abstract public class GraphServiceTestBase {

  abstract protected GraphService getGraphService() throws Exception;
  abstract protected void syncAfterWrite() throws Exception;

  @Test
  public void testAddEdge() throws Exception {
    GraphService client = getGraphService();

    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        "DownstreamOf");

    client.addEdge(edge1);
    syncAfterWrite();

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.OUTGOING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = client.findRelatedUrns(
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
    GraphService client = getGraphService();

    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "DownstreamOf");

    client.addEdge(edge1);
    syncAfterWrite();

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.INCOMING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = client.findRelatedUrns(
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
    GraphService client = getGraphService();

    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "DownstreamOf");

    client.addEdge(edge1);
    syncAfterWrite();

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.INCOMING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);

    client.removeEdgesFromNode(Urn.createFromString(
        "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        edgeTypes,
        relationshipFilter);
    syncAfterWrite();

    List<String> relatedUrnsPostDelete = client.findRelatedUrns(
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
