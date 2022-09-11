package com.linkedin.metadata.graph.elastic;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.ElasticSearchTestUtils;
import com.linkedin.metadata.ElasticTestUtils;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchServiceTest;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ElasticSearchGraphServiceTest extends GraphServiceTestBase {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention = new IndexConventionImpl(null);
  private final String _indexName = _indexConvention.getIndexName(INDEX_NAME);
  private ElasticSearchGraphService _client;

  @BeforeClass
  public void setup() {
    _elasticsearchContainer = ElasticTestUtils.getNewElasticsearchContainer();
    checkContainerEngine(_elasticsearchContainer.getDockerClient());
    _elasticsearchContainer.start();
    _searchClient = ElasticTestUtils.buildRestClient(_elasticsearchContainer);
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
    ESGraphQueryDAO readDAO = new ESGraphQueryDAO(_searchClient, lineageRegistry, _indexConvention);
    ESGraphWriteDAO writeDAO =
        new ESGraphWriteDAO(_searchClient, _indexConvention, ElasticSearchServiceTest.getBulkProcessor(_searchClient));
    return new ElasticSearchGraphService(lineageRegistry, _searchClient, _indexConvention, writeDAO, readDAO,
        ElasticSearchServiceTest.getIndexBuilder(_searchClient));
  }

  @AfterClass
  public void tearDown() {
    _elasticsearchContainer.stop();
  }

  @Override
  @Nonnull
  protected GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() throws Exception {
    ElasticSearchTestUtils.syncAfterWrite(_searchClient, _indexName);
  }

  @Override
  protected void assertEqualsAnyOrder(RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    // https://github.com/datahub-project/datahub/issues/3115
    // ElasticSearchGraphService produces duplicates, which is here ignored until fixed
    // actual.count and actual.total not tested due to duplicates
    assertEquals(actual.getStart(), expected.getStart());
    assertEqualsAnyOrder(actual.getEntities(), expected.getEntities(), RELATED_ENTITY_COMPARATOR);
  }

  @Override
  protected <T> void assertEqualsAnyOrder(List<T> actual, List<T> expected, Comparator<T> comparator) {
    // https://github.com/datahub-project/datahub/issues/3115
    // ElasticSearchGraphService produces duplicates, which is here ignored until fixed
    assertEquals(new HashSet<>(actual), new HashSet<>(expected));
  }

  @Override
  public void testFindRelatedEntitiesSourceEntityFilter(Filter sourceEntityFilter, List<String> relationshipTypes,
      RelationshipFilter relationships, List<RelatedEntity> expectedRelatedEntities) throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException("ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    super.testFindRelatedEntitiesSourceEntityFilter(sourceEntityFilter, relationshipTypes, relationships,
        expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesDestinationEntityFilter(Filter destinationEntityFilter,
      List<String> relationshipTypes, RelationshipFilter relationships, List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException("ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    super.testFindRelatedEntitiesDestinationEntityFilter(destinationEntityFilter, relationshipTypes, relationships,
        expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesSourceType(String datasetType, List<String> relationshipTypes,
      RelationshipFilter relationships, List<RelatedEntity> expectedRelatedEntities) throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException("ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3116
      throw new SkipException("ElasticSearchGraphService does not support empty source type");
    }
    super.testFindRelatedEntitiesSourceType(datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesDestinationType(String datasetType, List<String> relationshipTypes,
      RelationshipFilter relationships, List<RelatedEntity> expectedRelatedEntities) throws Exception {
    if (relationships.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException("ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3116
      throw new SkipException("ElasticSearchGraphService does not support empty destination type");
    }
    super.testFindRelatedEntitiesDestinationType(datasetType, relationshipTypes, relationships,
        expectedRelatedEntities);
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3117
    throw new SkipException("ElasticSearchGraphService does not support empty list of relationship types");
  }

  @Override
  public void testRemoveEdgesFromNode(@Nonnull Urn nodeToRemoveFrom, @Nonnull List<String> relationTypes,
      @Nonnull RelationshipFilter relationshipFilter, List<RelatedEntity> expectedOutgoingRelatedUrnsBeforeRemove,
      List<RelatedEntity> expectedIncomingRelatedUrnsBeforeRemove,
      List<RelatedEntity> expectedOutgoingRelatedUrnsAfterRemove,
      List<RelatedEntity> expectedIncomingRelatedUrnsAfterRemove) throws Exception {
    if (relationshipFilter.getDirection() == RelationshipDirection.UNDIRECTED) {
      // https://github.com/datahub-project/datahub/issues/3114
      throw new SkipException("ElasticSearchGraphService does not implement UNDIRECTED relationship filter");
    }
    super.testRemoveEdgesFromNode(nodeToRemoveFrom, relationTypes, relationshipFilter,
        expectedOutgoingRelatedUrnsBeforeRemove, expectedIncomingRelatedUrnsBeforeRemove,
        expectedOutgoingRelatedUrnsAfterRemove, expectedIncomingRelatedUrnsAfterRemove);
  }

  @Test
  @Override
  public void testRemoveEdgesFromNodeNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3117
    throw new SkipException("ElasticSearchGraphService does not support empty list of relationship types");
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
  public void testPopulatedGraphServiceGetLineageMultihop() throws Exception {
    GraphService service = getLineagePopulatedGraphService();

    EntityLineageResult upstreamLineage = service.getLineage(datasetOneUrn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineage.getTotal().intValue(), 0);
    assertEquals(upstreamLineage.getRelationships().size(), 0);

    EntityLineageResult downstreamLineage = service.getLineage(datasetOneUrn, LineageDirection.DOWNSTREAM, 0, 1000, 2);
    assertEquals(downstreamLineage.getTotal().intValue(), 5);
    assertEquals(downstreamLineage.getRelationships().size(), 5);
    Map<Urn, LineageRelationship> relationships = downstreamLineage.getRelationships().stream().collect(Collectors.toMap(LineageRelationship::getEntity,
        Function.identity()));
    assertTrue(relationships.containsKey(datasetTwoUrn));
    assertEquals(relationships.get(datasetTwoUrn).getDegree().intValue(), 1);
    assertTrue(relationships.containsKey(datasetThreeUrn));
    assertEquals(relationships.get(datasetThreeUrn).getDegree().intValue(), 2);
    assertTrue(relationships.containsKey(datasetFourUrn));
    assertEquals(relationships.get(datasetFourUrn).getDegree().intValue(), 2);
    assertTrue(relationships.containsKey(dataJobOneUrn));
    assertEquals(relationships.get(dataJobOneUrn).getDegree().intValue(), 1);
    assertTrue(relationships.containsKey(dataJobTwoUrn));
    assertEquals(relationships.get(dataJobTwoUrn).getDegree().intValue(), 1);

    upstreamLineage = service.getLineage(datasetThreeUrn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineage.getTotal().intValue(), 3);
    assertEquals(upstreamLineage.getRelationships().size(), 3);
    relationships = upstreamLineage.getRelationships().stream().collect(Collectors.toMap(LineageRelationship::getEntity,
        Function.identity()));
    assertTrue(relationships.containsKey(datasetOneUrn));
    assertEquals(relationships.get(datasetOneUrn).getDegree().intValue(), 2);
    assertTrue(relationships.containsKey(datasetTwoUrn));
    assertEquals(relationships.get(datasetTwoUrn).getDegree().intValue(), 1);
    assertTrue(relationships.containsKey(dataJobOneUrn));
    assertEquals(relationships.get(dataJobOneUrn).getDegree().intValue(), 1);

    downstreamLineage = service.getLineage(datasetThreeUrn, LineageDirection.DOWNSTREAM, 0, 1000, 2);
    assertEquals(downstreamLineage.getTotal().intValue(), 0);
    assertEquals(downstreamLineage.getRelationships().size(), 0);
  }
}
