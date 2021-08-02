package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static org.testng.Assert.assertEquals;


/**
 * Base class for testing any GraphService implementation.
 * Derive the test class from this base and get your GraphService implementation
 * tested with all these tests.
 *
 * You can add implementation specific tests in derived classes, or add general tests
 * here and have all existing implementations tested in the same way.
 */
abstract public class GraphServiceTestBase {

  /**
   * Provides the current GraphService instance to test. This is being called by the test method
   * at most once. The serviced graph should be empty.
   *
   * @return the GraphService instance to test
   * @throws Exception on failure
   */
  @Nonnull
  abstract protected GraphService getGraphService() throws Exception;

  /**
   * Allows the specific GraphService test implementation to wait for GraphService writes to
   * be synced / become available to reads.
   *
   * @throws Exception on failure
   */
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
