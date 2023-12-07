package com.linkedin.metadata.graph.search;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nonnull;
import org.junit.Assert;
import org.opensearch.client.RestHighLevelClient;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class SearchGraphServiceTestBase extends GraphServiceTestBase {

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  private final IndexConvention _indexConvention = new IndexConventionImpl(null);
  private final String _indexName = _indexConvention.getIndexName(INDEX_NAME);
  private ElasticSearchGraphService _client;

  private static final String TAG_RELATIONSHIP = "SchemaFieldTaggedWith";

  @BeforeClass
  public void setup() {
    _client = buildService();
    _client.configure();
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _client.clear();
    syncAfterWrite();
  }

  @Nonnull
  private ElasticSearchGraphService buildService() {
    LineageRegistry lineageRegistry = new LineageRegistry(SnapshotEntityRegistry.getInstance());
    ESGraphQueryDAO readDAO =
        new ESGraphQueryDAO(
            getSearchClient(),
            lineageRegistry,
            _indexConvention,
            GraphQueryConfiguration.testDefaults);
    ESGraphWriteDAO writeDAO = new ESGraphWriteDAO(_indexConvention, getBulkProcessor(), 1);
    return new ElasticSearchGraphService(
        lineageRegistry,
        getBulkProcessor(),
        _indexConvention,
        writeDAO,
        readDAO,
        getIndexBuilder());
  }

  @Override
  @Nonnull
  protected GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() throws Exception {
    SearchTestUtils.syncAfterWrite(getBulkProcessor());
  }

  @Override
  protected void assertEqualsAnyOrder(
      RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    // https://github.com/datahub-project/datahub/issues/3115
    // ElasticSearchGraphService produces duplicates, which is here ignored until fixed
    // actual.count and actual.total not tested due to duplicates
    assertEquals(actual.getStart(), expected.getStart());
    assertEqualsAnyOrder(actual.getEntities(), expected.getEntities(), RELATED_ENTITY_COMPARATOR);
  }

  @Override
  protected <T> void assertEqualsAnyOrder(
      List<T> actual, List<T> expected, Comparator<T> comparator) {
    // https://github.com/datahub-project/datahub/issues/3115
    // ElasticSearchGraphService produces duplicates, which is here ignored until fixed
    assertEquals(new HashSet<>(actual), new HashSet<>(expected));
  }

  @Override
  public void testFindRelatedEntitiesSourceEntityFilter(
      Filter sourceEntityFilter,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException(
          "ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    super.testFindRelatedEntitiesSourceEntityFilter(
        sourceEntityFilter, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesDestinationEntityFilter(
      Filter destinationEntityFilter,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException(
          "ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    super.testFindRelatedEntitiesDestinationEntityFilter(
        destinationEntityFilter, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesSourceType(
      String datasetType,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException(
          "ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3116
      throw new SkipException("ElasticSearchGraphService does not support empty source type");
    }
    super.testFindRelatedEntitiesSourceType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesDestinationType(
      String datasetType,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException(
          "ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3116
      throw new SkipException("ElasticSearchGraphService does not support empty destination type");
    }
    super.testFindRelatedEntitiesDestinationType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3117
    throw new SkipException(
        "ElasticSearchGraphService does not support empty list of relationship types");
  }

  @Override
  public void testRemoveEdgesFromNode(
      @Nonnull Urn nodeToRemoveFrom,
      @Nonnull List<String> relationTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      List<RelatedEntity> expectedOutgoingRelatedUrnsBeforeRemove,
      List<RelatedEntity> expectedIncomingRelatedUrnsBeforeRemove,
      List<RelatedEntity> expectedOutgoingRelatedUrnsAfterRemove,
      List<RelatedEntity> expectedIncomingRelatedUrnsAfterRemove)
      throws Exception {
    if (relationshipFilter.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException(
          "ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    super.testRemoveEdgesFromNode(
        nodeToRemoveFrom,
        relationTypes,
        relationshipFilter,
        expectedOutgoingRelatedUrnsBeforeRemove,
        expectedIncomingRelatedUrnsBeforeRemove,
        expectedOutgoingRelatedUrnsAfterRemove,
        expectedIncomingRelatedUrnsAfterRemove);
  }

  @Test
  @Override
  public void testRemoveEdgesFromNodeNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3117
    throw new SkipException(
        "ElasticSearchGraphService does not support empty list of relationship types");
  }

  @Test
  // TODO: Only in ES for now since unimplemented in other services
  public void testRemoveEdge() throws Exception {
    DatasetUrn datasetUrn =
        new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.TEST);
    TagUrn tagUrn = new TagUrn("newTag");
    Edge edge = new Edge(datasetUrn, tagUrn, TAG_RELATIONSHIP, null, null, null, null, null);
    getGraphService().addEdge(edge);
    syncAfterWrite();
    RelatedEntitiesResult result =
        getGraphService()
            .findRelatedEntities(
                Collections.singletonList(datasetType),
                newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
                Collections.singletonList("tag"),
                EMPTY_FILTER,
                Collections.singletonList(TAG_RELATIONSHIP),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
                0,
                100);
    assertEquals(result.getTotal(), 1);
    getGraphService().removeEdge(edge);
    syncAfterWrite();
    result =
        getGraphService()
            .findRelatedEntities(
                Collections.singletonList(datasetType),
                newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
                Collections.singletonList("tag"),
                EMPTY_FILTER,
                Collections.singletonList(TAG_RELATIONSHIP),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
                0,
                100);
    assertEquals(result.getTotal(), 0);
  }

  @Test
  @Override
  public void testConcurrentAddEdge() {
    // https://github.com/datahub-project/datahub/issues/3124
    throw new SkipException(
        "This test is flaky for ElasticSearchGraphService, ~5% of the runs fail on a race condition");
  }

  @Test
  @Override
  public void testConcurrentRemoveEdgesFromNode() {
    // https://github.com/datahub-project/datahub/issues/3118
    throw new SkipException("ElasticSearchGraphService produces duplicates");
  }

  @Test
  @Override
  public void testConcurrentRemoveNodes() {
    // https://github.com/datahub-project/datahub/issues/3118
    throw new SkipException("ElasticSearchGraphService produces duplicates");
  }

  @Test
  public void testTimestampLineage() throws Exception {
    // Populate one upstream and two downstream edges at initialTime
    Long initialTime = 1000L;

    List<Edge> edges =
        Arrays.asList(
            // One upstream edge
            new Edge(
                datasetTwoUrn,
                datasetOneUrn,
                downstreamOf,
                initialTime,
                null,
                initialTime,
                null,
                null),
            // Two downstream
            new Edge(
                datasetThreeUrn,
                datasetTwoUrn,
                downstreamOf,
                initialTime,
                null,
                initialTime,
                null,
                null),
            new Edge(
                datasetFourUrn,
                datasetTwoUrn,
                downstreamOf,
                initialTime,
                null,
                initialTime,
                null,
                null),
            // One with null values, should always be returned
            new Edge(datasetFiveUrn, datasetTwoUrn, downstreamOf, null, null, null, null, null));

    edges.forEach(getGraphService()::addEdge);
    syncAfterWrite();

    // Without timestamps
    EntityLineageResult upstreamResult = getUpstreamLineage(datasetTwoUrn, null, null);
    EntityLineageResult downstreamResult = getDownstreamLineage(datasetTwoUrn, null, null);
    Assert.assertEquals(new Integer(1), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(3), downstreamResult.getTotal());

    // Timestamp before
    upstreamResult = getUpstreamLineage(datasetTwoUrn, 0L, initialTime - 10);
    downstreamResult = getDownstreamLineage(datasetTwoUrn, 0L, initialTime - 10);
    Assert.assertEquals(new Integer(0), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(1), downstreamResult.getTotal());

    // Timestamp after
    upstreamResult = getUpstreamLineage(datasetTwoUrn, initialTime + 10, initialTime + 100);
    downstreamResult = getDownstreamLineage(datasetTwoUrn, initialTime + 10, initialTime + 100);
    Assert.assertEquals(new Integer(0), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(1), downstreamResult.getTotal());

    // Timestamp included
    upstreamResult = getUpstreamLineage(datasetTwoUrn, initialTime - 10, initialTime + 10);
    downstreamResult = getDownstreamLineage(datasetTwoUrn, initialTime - 10, initialTime + 10);
    Assert.assertEquals(new Integer(1), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(3), downstreamResult.getTotal());

    // Update only one of the downstream edges
    Long updatedTime = 2000L;
    edges =
        Arrays.asList(
            new Edge(
                datasetTwoUrn,
                datasetOneUrn,
                downstreamOf,
                initialTime,
                null,
                updatedTime,
                null,
                null),
            new Edge(
                datasetThreeUrn,
                datasetTwoUrn,
                downstreamOf,
                initialTime,
                null,
                updatedTime,
                null,
                null));

    edges.forEach(getGraphService()::addEdge);
    syncAfterWrite();

    // Without timestamps
    upstreamResult = getUpstreamLineage(datasetTwoUrn, null, null);
    downstreamResult = getDownstreamLineage(datasetTwoUrn, null, null);
    Assert.assertEquals(new Integer(1), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(3), downstreamResult.getTotal());

    // Window includes initial time and updated time
    upstreamResult = getUpstreamLineage(datasetTwoUrn, initialTime - 10, updatedTime + 10);
    downstreamResult = getDownstreamLineage(datasetTwoUrn, initialTime - 10, updatedTime + 10);
    Assert.assertEquals(new Integer(1), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(3), downstreamResult.getTotal());

    // Window includes updated time but not initial time
    upstreamResult = getUpstreamLineage(datasetTwoUrn, initialTime + 10, updatedTime + 10);
    downstreamResult = getDownstreamLineage(datasetTwoUrn, initialTime + 10, updatedTime + 10);
    Assert.assertEquals(new Integer(1), upstreamResult.getTotal());
    Assert.assertEquals(new Integer(2), downstreamResult.getTotal());
  }

  /**
   * Utility method to reduce repeated parameters for lineage tests
   *
   * @param urn URN to query
   * @param startTime Start of time-based lineage query
   * @param endTime End of time-based lineage query
   * @return The Upstream lineage for urn from the window from startTime to endTime
   */
  private EntityLineageResult getUpstreamLineage(Urn urn, Long startTime, Long endTime) {
    return getLineage(urn, LineageDirection.UPSTREAM, startTime, endTime);
  }

  /**
   * Utility method to reduce repeated parameters for lineage tests
   *
   * @param urn URN to query
   * @param startTime Start of time-based lineage query
   * @param endTime End of time-based lineage query
   * @return The Downstream lineage for urn from the window from startTime to endTime
   */
  private EntityLineageResult getDownstreamLineage(Urn urn, Long startTime, Long endTime) {
    return getLineage(urn, LineageDirection.DOWNSTREAM, startTime, endTime);
  }

  /**
   * Utility method to reduce repeated parameters for lineage tests
   *
   * @param urn URN to query
   * @param direction Direction to query (upstream/downstream)
   * @param startTime Start of time-based lineage query
   * @param endTime End of time-based lineage query
   * @return The lineage for urn from the window from startTime to endTime in direction
   */
  private EntityLineageResult getLineage(
      Urn urn, LineageDirection direction, Long startTime, Long endTime) {
    return getGraphService().getLineage(urn, direction, 0, 0, 3, startTime, endTime);
  }
}
