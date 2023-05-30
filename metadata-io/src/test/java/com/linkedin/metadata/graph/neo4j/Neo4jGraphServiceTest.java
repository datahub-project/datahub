package com.linkedin.metadata.graph.neo4j;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import java.util.Arrays;
import java.util.Collections;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.testng.Assert.assertEquals;


public class Neo4jGraphServiceTest extends GraphServiceTestBase {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jGraphService _client;

  private static final String TAG_RELATIONSHIP = "SchemaFieldTaggedWith";

  @BeforeClass
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());
    _client = new Neo4jGraphService(new LineageRegistry(SnapshotEntityRegistry.getInstance()), _driver);
    _client.clear();
  }

  @BeforeMethod
  public void wipe() {
    _client.wipe();
  }

  @AfterClass
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Override
  protected @Nonnull
  GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() {
  }

  @Override
  protected void assertEqualsAnyOrder(RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    // https://github.com/datahub-project/datahub/issues/3118
    // Neo4jGraphService produces duplicates, which is here ignored until fixed
    // actual.count and actual.total not tested due to duplicates
    assertEquals(actual.getStart(), expected.getStart());
    assertEqualsAnyOrder(actual.getEntities(), expected.getEntities(), RELATED_ENTITY_COMPARATOR);
  }

  @Override
  protected <T> void assertEqualsAnyOrder(List<T> actual, List<T> expected, Comparator<T> comparator) {
    // https://github.com/datahub-project/datahub/issues/3118
    // Neo4jGraphService produces duplicates, which is here ignored until fixed
    assertEquals(
            new HashSet<>(actual),
            new HashSet<>(expected)
    );
  }

  @Override
  public void testFindRelatedEntitiesSourceType(String datasetType,
                                                List<String> relationshipTypes,
                                                RelationshipFilter relationships,
                                                List<RelatedEntity> expectedRelatedEntities) throws Exception {
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3119
      throw new SkipException("Neo4jGraphService does not support empty source type");
    }
    if (datasetType != null && datasetType.equals(GraphServiceTestBase.userType)) {
      // https://github.com/datahub-project/datahub/issues/3123
      // only test cases with "user" type fail due to this bug
      throw new SkipException("Neo4jGraphService does not apply source / destination types");
    }
    super.testFindRelatedEntitiesSourceType(datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  public void testFindRelatedEntitiesDestinationType(String datasetType,
                                                     List<String> relationshipTypes,
                                                     RelationshipFilter relationships,
                                                     List<RelatedEntity> expectedRelatedEntities) throws Exception {
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3119
      throw new SkipException("Neo4jGraphService does not support empty destination type");
    }
    if (relationshipTypes.contains(hasOwner)) {
      // https://github.com/datahub-project/datahub/issues/3123
      // only test cases with "HasOwner" relatioship fail due to this bug
      throw new SkipException("Neo4jGraphService does not apply source / destination types");
    }
    super.testFindRelatedEntitiesDestinationType(datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNullSourceType() throws Exception {
    // https://github.com/datahub-project/datahub/issues/3121
    throw new SkipException("Neo4jGraphService does not support 'null' entity type string");
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNullDestinationType() throws Exception {
    // https://github.com/datahub-project/datahub/issues/3121
    throw new SkipException("Neo4jGraphService does not support 'null' entity type string");
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3120
    throw new SkipException("Neo4jGraphService does not support empty list of relationship types");
  }

  @Test
  @Override
  public void testRemoveEdgesFromNodeNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3120
    throw new SkipException("Neo4jGraphService does not support empty list of relationship types");
  }

  @Test
  @Override
  public void testConcurrentAddEdge() {
    // https://github.com/datahub-project/datahub/issues/3141
    throw new SkipException("Neo4jGraphService does not manage to add all edges added concurrently");
  }

  @Test
  @Override
  public void testConcurrentRemoveEdgesFromNode() {
    // https://github.com/datahub-project/datahub/issues/3118
    throw new SkipException("Neo4jGraphService produces duplicates");
  }

  @Test
  @Override
  public void testConcurrentRemoveNodes() {
    // https://github.com/datahub-project/datahub/issues/3118
    throw new SkipException("Neo4jGraphService produces duplicates");
  }

  @Test
  public void testRemoveEdge() throws Exception {
    DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.TEST);
    TagUrn tagUrn = new TagUrn("newTag");
    Edge edge = new Edge(datasetUrn, tagUrn, TAG_RELATIONSHIP, null, null, null, null, null);
    getGraphService().addEdge(edge);

    RelatedEntitiesResult result = getGraphService().findRelatedEntities(Collections.singletonList(datasetType),
        newFilter(Collections.singletonMap("urn", datasetUrn.toString())), Collections.singletonList("tag"),
        EMPTY_FILTER, Collections.singletonList(TAG_RELATIONSHIP),
        newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 0, 100);
    assertEquals(result.getTotal(), 1);
    getGraphService().removeEdge(edge);

    result = getGraphService().findRelatedEntities(Collections.singletonList(datasetType),
        newFilter(Collections.singletonMap("urn", datasetUrn.toString())), Collections.singletonList("tag"),
        EMPTY_FILTER, Collections.singletonList(TAG_RELATIONSHIP),
        newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 0, 100);
    assertEquals(result.getTotal(), 0);
  }

  @Test
  public void testGetLineageTimeFilterQuery() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges = Arrays.asList(
        new Edge(dataJobOneUrn, datasetOneUrn, consumes, 1L, null, 3L, null, null),
        new Edge(dataJobOneUrn, datasetTwoUrn, produces, 5L, null, 7L, null, null),
        new Edge(datasetThreeUrn, datasetTwoUrn, downstreamOf, 9L, null, null, null, null),
        new Edge(datasetFourUrn, datasetThreeUrn, downstreamOf, 11L, null, null, null, null)
    );
    edges.forEach(service::addEdge);

    EntityLineageResult upstreamLineageTwoHops = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineageTwoHops.getTotal().intValue(), 2);
    assertEquals(upstreamLineageTwoHops.getRelationships().size(), 2);

    EntityLineageResult upstreamLineageTwoHopsWithTimeFilter = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 1000, 2, 10L, 12L);
    assertEquals(upstreamLineageTwoHopsWithTimeFilter.getTotal().intValue(), 1);
    assertEquals(upstreamLineageTwoHopsWithTimeFilter.getRelationships().size(), 1);

    EntityLineageResult upstreamLineageTimeFilter = service.getLineage(datasetTwoUrn, LineageDirection.UPSTREAM, 0, 1000, 4, 2L, 6L);
    assertEquals(upstreamLineageTimeFilter.getTotal().intValue(), 2);
    assertEquals(upstreamLineageTimeFilter.getRelationships().size(), 2);

    EntityLineageResult downstreamLineageTimeFilter = service.getLineage(datasetOneUrn, LineageDirection.DOWNSTREAM, 0, 1000, 4, 0L, 4L);
    assertEquals(downstreamLineageTimeFilter.getTotal().intValue(), 1);
    assertEquals(downstreamLineageTimeFilter.getRelationships().size(), 1);

  }
}
