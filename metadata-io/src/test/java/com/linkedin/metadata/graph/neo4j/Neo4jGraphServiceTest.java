package com.linkedin.metadata.graph.neo4j;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipFilter;
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

import static org.testng.Assert.assertEquals;


public class Neo4jGraphServiceTest extends GraphServiceTestBase {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jGraphService _client;

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

}
