package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
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
 *
 * Note this base class does not test GraphService.addEdge explicitly. This method is tested
 * indirectly by all tests via `getPopulatedGraphService` at the beginning of each test.
 * The `getPopulatedGraphService` method calls `GraphService.addEdge` to populate the Graph.
 * Feel free to add a test to your test implementation that calls `getPopulatedGraphService` and
 * asserts the state of the graph in an implementation specific way.
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

  /**
   * Calls getGraphService to retrieve the test GraphService and populates it
   * with edges via `GraphService.addEdge`.
   *
   * @return test GraphService
   * @throws Exception on failure
   */
  protected GraphService getPopulatedGraphService() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges = Arrays.asList(
            new Edge(
                    Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
                    Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
                    "DownstreamOf"
            )
    );

    edges.forEach(service::addEdge);
    syncAfterWrite();

    return service;
  }

  protected RelationshipFilter createRelationshipFilter(RelationshipDirection direction) {
    return createRelationshipFilter(EMPTY_FILTER, direction);
  }

  protected RelationshipFilter createRelationshipFilter(@Nonnull Filter filter, @Nonnull RelationshipDirection direction) {
    return new RelationshipFilter()
            .setCriteria(filter.getCriteria())
            .setDirection(direction);
  }

  @Test
  public void testFindRelatedUrns() throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> relatedUrns = service.findRelatedUrns(
            "",
            newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
            "",
            EMPTY_FILTER,
            Arrays.asList("DownstreamOf"),
            createRelationshipFilter(RelationshipDirection.OUTGOING),
            0,
            10);

    assertEquals(relatedUrns.size(), 1);
  }

  @Test
  public void testFindRelatedUrnsReverse() throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> relatedUrns = service.findRelatedUrns(
            "",
            newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
            "",
            EMPTY_FILTER,
            Arrays.asList("DownstreamOf"),
            createRelationshipFilter(RelationshipDirection.INCOMING),
            0,
            10);

    assertEquals(relatedUrns.size(), 1);
  }

  @Test
  public void testRemoveEdgesFromNode() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)");
    doTestRemoveEdgesFromNodeReverse(urn, RelationshipDirection.OUTGOING);
  }

  @Test
  public void testRemoveEdgesFromNodeReverse() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
    doTestRemoveEdgesFromNodeReverse(urn, RelationshipDirection.INCOMING);
  }

  public void doTestRemoveEdgesFromNodeReverse(@Nonnull Urn nodeToDelete,
                                               @Nonnull RelationshipDirection direction) throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> edgeTypes = Arrays.asList("DownstreamOf");
    RelationshipFilter relationshipFilter = createRelationshipFilter(direction);

    List<String> relatedUrns = service.findRelatedUrns(
            "",
            newFilter("urn", nodeToDelete.toString()),
            "",
            EMPTY_FILTER,
            edgeTypes,
            createRelationshipFilter(direction),
            0,
            10);
    assertEquals(relatedUrns.size(), 1);

    service.removeEdgesFromNode(Urn.createFromString(
        nodeToDelete.toString()),
        edgeTypes,
        relationshipFilter);
    syncAfterWrite();

    List<String> relatedUrnsPostDelete = service.findRelatedUrns(
            "",
            newFilter("urn", nodeToDelete.toString()),
            "",
            EMPTY_FILTER,
            edgeTypes,
            relationshipFilter,
            0,
            10);

    assertEquals(relatedUrnsPostDelete.size(), 0);
  }
}
