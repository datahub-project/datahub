package com.linkedin.metadata.graph.search;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

  private final IndexConvention _indexConvention = IndexConventionImpl.noPrefix("MD5");
  private final String _indexName = _indexConvention.getIndexName(INDEX_NAME);
  private ElasticSearchGraphService _client;
  private OperationContext operationContext;

  private static final String TAG_RELATIONSHIP = "SchemaFieldTaggedWith";

  @BeforeClass
  public void setup() {
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    _client = buildService(_graphQueryConfiguration.isEnableMultiPathSearch());
    _client.reindexAll(Collections.emptySet());
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite();
    _client.clear();
    syncAfterWrite();
  }

  @Nonnull
  private ElasticSearchGraphService buildService(boolean enableMultiPathSearch) {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            SearchCommonTestConfiguration.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));
    SnapshotEntityRegistry snapshotEntityRegistry = SnapshotEntityRegistry.getInstance();
    LineageRegistry lineageRegistry;
    try {
      MergedEntityRegistry mergedEntityRegistry =
          new MergedEntityRegistry(snapshotEntityRegistry).apply(configEntityRegistry);
      lineageRegistry = new LineageRegistry(mergedEntityRegistry);
    } catch (EntityRegistryException e) {
      throw new RuntimeException(e);
    }
    _graphQueryConfiguration.setEnableMultiPathSearch(enableMultiPathSearch);
    ESGraphQueryDAO readDAO =
        new ESGraphQueryDAO(
            getSearchClient(), lineageRegistry, _indexConvention, _graphQueryConfiguration);
    ESGraphWriteDAO writeDAO =
        new ESGraphWriteDAO(_indexConvention, getBulkProcessor(), 1, _graphQueryConfiguration);
    return new ElasticSearchGraphService(
        lineageRegistry,
        getBulkProcessor(),
        _indexConvention,
        writeDAO,
        readDAO,
        getIndexBuilder(),
        "MD5");
  }

  @Override
  @Nonnull
  protected GraphService getGraphService(boolean enableMultiPathSearch) {
    if (enableMultiPathSearch != _graphQueryConfiguration.isEnableMultiPathSearch()) {
      _client = buildService(enableMultiPathSearch);
      _client.reindexAll(Collections.emptySet());
    }
    return _client;
  }

  @Override
  @Nonnull
  protected GraphService getGraphService() {
    return getGraphService(_graphQueryConfiguration.isEnableMultiPathSearch());
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
                operationContext,
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
                operationContext,
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

  // ElasticSearchGraphService produces duplicates
  // https://github.com/datahub-project/datahub/issues/3118
  protected Set<RelatedEntity> deduplicateRelatedEntitiesByRelationshipTypeAndDestination(
      RelatedEntitiesResult relatedEntitiesResult) {
    return relatedEntitiesResult.getEntities().stream()
        .map(
            relatedEntity ->
                new RelatedEntity(relatedEntity.getRelationshipType(), relatedEntity.getUrn()))
        .collect(Collectors.toSet());
  }

  @Test
  public void testTimestampLineage() throws Exception {
    // Populate one upstream and two downstream edges at initialTime
    Long initialTime = 1000L;

    List<Edge> edges =
        Arrays.asList(
            // One upstream edge
            new Edge(
                dataset2Urn, dataset1Urn, downstreamOf, initialTime, null, initialTime, null, null),
            // Two downstream
            new Edge(
                dataset3Urn, dataset2Urn, downstreamOf, initialTime, null, initialTime, null, null),
            new Edge(
                dataset4Urn, dataset2Urn, downstreamOf, initialTime, null, initialTime, null, null),
            // One with null values, should always be returned
            new Edge(dataset5Urn, dataset2Urn, downstreamOf, null, null, null, null, null));

    edges.forEach(getGraphService()::addEdge);
    syncAfterWrite();

    // Without timestamps
    EntityLineageResult upstreamResult = getUpstreamLineage(dataset2Urn, null, null);
    EntityLineageResult downstreamResult = getDownstreamLineage(dataset2Urn, null, null);
    Assert.assertEquals(Integer.valueOf(1), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(3), downstreamResult.getTotal());

    // Timestamp before
    upstreamResult = getUpstreamLineage(dataset2Urn, 0L, initialTime - 10);
    downstreamResult = getDownstreamLineage(dataset2Urn, 0L, initialTime - 10);
    Assert.assertEquals(Integer.valueOf(0), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(1), downstreamResult.getTotal());

    // Timestamp after
    upstreamResult = getUpstreamLineage(dataset2Urn, initialTime + 10, initialTime + 100);
    downstreamResult = getDownstreamLineage(dataset2Urn, initialTime + 10, initialTime + 100);
    Assert.assertEquals(Integer.valueOf(0), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(1), downstreamResult.getTotal());

    // Timestamp included
    upstreamResult = getUpstreamLineage(dataset2Urn, initialTime - 10, initialTime + 10);
    downstreamResult = getDownstreamLineage(dataset2Urn, initialTime - 10, initialTime + 10);
    Assert.assertEquals(Integer.valueOf(1), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(3), downstreamResult.getTotal());

    // Update only one of the downstream edges
    Long updatedTime = 2000L;
    edges =
        Arrays.asList(
            new Edge(
                dataset2Urn, dataset1Urn, downstreamOf, initialTime, null, updatedTime, null, null),
            new Edge(
                dataset3Urn,
                dataset2Urn,
                downstreamOf,
                initialTime,
                null,
                updatedTime,
                null,
                null));

    edges.forEach(getGraphService()::addEdge);
    syncAfterWrite();

    // Without timestamps
    upstreamResult = getUpstreamLineage(dataset2Urn, null, null);
    downstreamResult = getDownstreamLineage(dataset2Urn, null, null);
    Assert.assertEquals(Integer.valueOf(1), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(3), downstreamResult.getTotal());

    // Window includes initial time and updated time
    upstreamResult = getUpstreamLineage(dataset2Urn, initialTime - 10, updatedTime + 10);
    downstreamResult = getDownstreamLineage(dataset2Urn, initialTime - 10, updatedTime + 10);
    Assert.assertEquals(Integer.valueOf(1), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(3), downstreamResult.getTotal());

    // Window includes updated time but not initial time
    upstreamResult = getUpstreamLineage(dataset2Urn, initialTime + 10, updatedTime + 10);
    downstreamResult = getDownstreamLineage(dataset2Urn, initialTime + 10, updatedTime + 10);
    Assert.assertEquals(Integer.valueOf(1), upstreamResult.getTotal());
    Assert.assertEquals(Integer.valueOf(2), downstreamResult.getTotal());
  }

  @Test
  public void testExplored() throws Exception {

    List<Edge> edges =
        Arrays.asList(
            // One upstream edge
            new Edge(dataset2Urn, dataset1Urn, downstreamOf, null, null, null, null, null),
            // Two downstream
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset4Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            // One with null values, should always be returned
            new Edge(dataset5Urn, dataset2Urn, downstreamOf, null, null, null, null, null));

    edges.forEach(getGraphService()::addEdge);
    syncAfterWrite();

    EntityLineageResult result = getUpstreamLineage(dataset2Urn, null, null, 10);
    Assert.assertTrue(Boolean.TRUE.equals(result.getRelationships().get(0).isExplored()));

    EntityLineageResult result2 = getUpstreamLineage(dataset2Urn, null, null, 10, 0);
    Assert.assertTrue(result2.getRelationships().isEmpty());

    EntityLineageResult result3 = getUpstreamLineage(dataset2Urn, null, null, 10, 1);
    Assert.assertTrue(result3.getRelationships().get(0).isExplored());
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
    return getLineage(urn, LineageDirection.UPSTREAM, startTime, endTime, 0, null);
  }

  private EntityLineageResult getUpstreamLineage(Urn urn, Long startTime, Long endTime, int count) {
    return getLineage(urn, LineageDirection.UPSTREAM, startTime, endTime, count, null);
  }

  private EntityLineageResult getUpstreamLineage(
      Urn urn, Long startTime, Long endTime, int count, int exploreLimit) {
    return getLineage(urn, LineageDirection.UPSTREAM, startTime, endTime, count, exploreLimit);
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
    return getLineage(urn, LineageDirection.DOWNSTREAM, startTime, endTime, 0, null);
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
      Urn urn,
      LineageDirection direction,
      Long startTime,
      Long endTime,
      int count,
      @Nullable Integer entitiesExploredPerHopLimit) {
    return getGraphService()
        .getLineage(
            operationContext.withLineageFlags(
                f ->
                    new LineageFlags()
                        .setStartTimeMillis(startTime, SetMode.REMOVE_IF_NULL)
                        .setEndTimeMillis(endTime, SetMode.REMOVE_IF_NULL)
                        .setEntitiesExploredPerHopLimit(
                            entitiesExploredPerHopLimit, SetMode.REMOVE_IF_NULL)),
            urn,
            direction,
            0,
            count,
            3);
  }
}
