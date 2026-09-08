package com.linkedin.metadata.graph.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBaseNoVia;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link PostgresGraphService} against PostgreSQL (Testcontainers).
 *
 * <p>Uses {@link GraphServiceTestBaseNoVia}: one-hop results can include lifecycle/via variants
 * that Elasticsearch treats as distinct {@code RelatedEntity} rows; assertions follow Neo4j's
 * via-less contract, plus an explicit via round-trip below.
 */
public class PostgresGraphServiceIT extends GraphServiceTestBaseNoVia {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private Database database;
  private PostgresGraphService graphService;
  private PostgresGraphTables tables;

  @BeforeClass
  public void init() throws Exception {
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("pggraph");
    PostgresSqlSetupProperties props =
        PostgresTestUtils.testPgGraphProperties(ns.getSchema(), ns.getTablePrefix());
    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgres();
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pggraph_it"));
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.applyPgGraphSchema(c, props);
    }

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

    GraphServiceConfiguration graphConfig =
        TEST_GRAPH_SERVICE_CONFIG.toBuilder().type("postgres").build();
    PostgresGraphTables tables = new PostgresGraphTables(props);
    this.tables = tables;
    PostgresGraphWriteDao writeSink = new PostgresGraphWriteDao(database, props);
    PostgresGraphOneHopDao oneHop = new PostgresGraphOneHopDao(database, tables);
    PostgresGraphPgRoutingDao pgRouting =
        new PostgresGraphPgRoutingDao(database, tables, lineageRegistry);
    PostgresGraphLineageDao lineage =
        new PostgresGraphLineageDao(oneHop, pgRouting, lineageRegistry, graphConfig);

    graphService =
        new PostgresGraphService(graphConfig, lineageRegistry, writeSink, oneHop, lineage);
    graphService.clear(OP_CONTEXT);
  }

  @BeforeMethod
  public void wipe() {
    graphService.clear(OP_CONTEXT);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @Override
  protected @Nonnull GraphService getGraphService() {
    return graphService;
  }

  @Override
  protected void syncAfterWrite() {}

  @Override
  protected void assertEqualsAnyOrder(
      RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    assertEquals(
        relatedEntityKeys(actual.getEntities()), relatedEntityKeys(expected.getEntities()));
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> void assertEqualsAnyOrder(List<T> actual, List<T> expected) {
    if (!actual.isEmpty() && actual.get(0) instanceof RelatedEntity) {
      assertEquals(
          relatedEntityKeys((List<RelatedEntity>) actual),
          relatedEntityKeys((List<RelatedEntity>) expected));
      return;
    }
    super.assertEqualsAnyOrder(actual, expected);
  }

  private static Set<String> relatedEntityKeys(List<RelatedEntity> entities) {
    return entities.stream()
        .map(e -> e.getRelationshipType() + "\0" + e.getUrn())
        .collect(Collectors.toSet());
  }

  @Override
  @Test(dataProvider = "NoViaFindRelatedEntitiesSourceTypeTests")
  public void testFindRelatedEntitiesSourceType(
      String datasetType,
      Set<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    super.testFindRelatedEntitiesSourceType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  @Test(dataProvider = "NoViaFindRelatedEntitiesDestinationTypeTests")
  public void testFindRelatedEntitiesDestinationType(
      String datasetType,
      Set<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    super.testFindRelatedEntitiesDestinationType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  @Test
  public void testConcurrentAddEdge() {
    throw new SkipException(
        "PostgresGraphWriteDao: concurrent addEdge with synthetic URNs is not isolated for IT");
  }

  @Override
  @Test
  public void testConcurrentRemoveEdgesFromNode() {
    throw new SkipException(
        "Depends on testConcurrentAddEdge fully populating the graph; skipped for same reason");
  }

  @Override
  @Test
  public void testConcurrentRemoveNodes() {
    throw new SkipException(
        "Depends on testConcurrentAddEdge fully populating the graph; skipped for same reason");
  }

  @Override
  @Test
  public void testHighlyConnectedGraphWalk() {
    throw new SkipException(
        "Uses concurrent addEdge; skipped for the same isolation reason as testConcurrentAddEdge");
  }

  @Test
  public void testImpactLineageTypedHopsIgnoreNonLineageEdges() throws Exception {
    GraphService service = getLineagePopulatedGraphService();
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            service.getLineageRegistry(), dataset1Urn.getEntityType(), LineageDirection.DOWNSTREAM);

    EntityLineageResult hop1 = service.getImpactLineage(operationContext, dataset1Urn, filters, 1);
    Set<String> hop1Urns =
        hop1.getRelationships().stream()
            .map(rel -> rel.getEntity().toString())
            .collect(Collectors.toSet());
    assertTrue(hop1Urns.contains(dataset2Urn.toString()));
    assertFalse(hop1Urns.contains(dataset3Urn.toString()));
    assertFalse(hop1Urns.contains(userOneUrn.toString()));
    for (LineageRelationship rel : hop1.getRelationships()) {
      assertFalse(hasOwner.equals(rel.getType()));
    }

    EntityLineageResult hop2 = service.getImpactLineage(operationContext, dataset1Urn, filters, 2);
    Set<String> hop2Urns =
        hop2.getRelationships().stream()
            .map(rel -> rel.getEntity().toString())
            .collect(Collectors.toSet());
    assertTrue(hop2Urns.contains(dataset2Urn.toString()));
    assertTrue(hop2Urns.contains(dataset3Urn.toString()));
    assertFalse(hop2Urns.contains(userOneUrn.toString()));
    assertEquals(hop2.getRelationships().size(), hop2Urns.size());
  }

  @Test
  public void testConnectedComponentsStayEmptyWhenFlagOff() throws Exception {
    graphService.addEdge(
        operationContext,
        new Edge(dataset2Urn, dataset1Urn, downstreamOf, null, null, null, null, null));
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement()) {
      try (ResultSet defs = st.executeQuery("SELECT COUNT(*) FROM " + tables.ccDefinitions())) {
        assertTrue(defs.next());
        assertEquals(defs.getInt(1), 0);
      }
      try (ResultSet verts = st.executeQuery("SELECT COUNT(*) FROM " + tables.ccVertices())) {
        assertTrue(verts.next());
        assertEquals(verts.getInt(1), 0);
      }
    }
  }

  @Test
  public void testImpactLineageQueryViaPathAndViaEntity() throws Exception {
    Urn queryUrn = Urn.createFromString("urn:li:query:pggraph_via_q1");
    graphService.addEdge(
        operationContext,
        new Edge(dataset2Urn, dataset1Urn, downstreamOf, 0L, null, 0L, null, null, null, queryUrn));
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            graphService.getLineageRegistry(),
            dataset1Urn.getEntityType(),
            LineageDirection.DOWNSTREAM);

    EntityLineageResult impact =
        graphService.getImpactLineage(operationContext, dataset1Urn, filters, 1);
    LineageRelationship dest = relationship(impact, dataset2Urn);
    assertEquals(dest.getPaths().get(0).size(), 3);
    assertEquals(dest.getPaths().get(0).get(1), queryUrn);
    assertEquals(relationship(impact, queryUrn).getPaths().get(0).get(1), queryUrn);

    EntityLineageResult ui =
        graphService.getLineage(operationContext, dataset1Urn, filters, 0, 100, 1);
    assertEquals(relationship(ui, dataset2Urn).getPaths().get(0).get(1), queryUrn);
  }

  @Test
  public void testGetLineageCarriesViaOnSecondHop() throws Exception {
    Urn q1 = Urn.createFromString("urn:li:query:pggraph_via_hop1");
    Urn q2 = Urn.createFromString("urn:li:query:pggraph_via_hop2");
    graphService.addEdge(
        operationContext,
        new Edge(dataset2Urn, dataset1Urn, downstreamOf, 0L, null, 0L, null, null, null, q1));
    graphService.addEdge(
        operationContext,
        new Edge(dataset3Urn, dataset2Urn, downstreamOf, 0L, null, 0L, null, null, null, q2));
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            graphService.getLineageRegistry(),
            dataset1Urn.getEntityType(),
            LineageDirection.DOWNSTREAM);
    LineageRelationship hop2 =
        relationship(
            graphService.getLineage(operationContext, dataset1Urn, filters, 0, 100, 2),
            dataset3Urn);
    assertEquals(hop2.getPaths().get(0).size(), 5);
    assertEquals(hop2.getPaths().get(0).get(1), q1);
    assertEquals(hop2.getPaths().get(0).get(2), dataset2Urn);
    assertEquals(hop2.getPaths().get(0).get(3), q2);
    assertEquals(hop2.getPaths().get(0).get(4), dataset3Urn);
  }

  @Test
  public void testImpactAndUiLineageJobAsNodeNoInventedVia() throws Exception {
    graphService.addEdge(
        operationContext,
        new Edge(dataJobOneUrn, dataset1Urn, consumes, null, null, null, null, null));
    graphService.addEdge(
        operationContext,
        new Edge(dataJobOneUrn, dataset2Urn, produces, null, null, null, null, null));
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            graphService.getLineageRegistry(),
            dataset1Urn.getEntityType(),
            LineageDirection.DOWNSTREAM);

    EntityLineageResult impact =
        graphService.getImpactLineage(operationContext, dataset1Urn, filters, 2);
    assertEquals(relationship(impact, dataJobOneUrn).getPaths().get(0).size(), 2);
    assertEquals(relationship(impact, dataset2Urn).getPaths().get(0).size(), 3);
    assertEquals(relationship(impact, dataset2Urn).getPaths().get(0).get(1), dataJobOneUrn);
    assertFalse(impact.getRelationships().stream().anyMatch(r -> r.getPaths().get(0).size() == 4));

    EntityLineageResult ui =
        graphService.getLineage(operationContext, dataset1Urn, filters, 0, 100, 2);
    assertEquals(relationship(ui, dataset2Urn).getPaths().get(0).get(1), dataJobOneUrn);
  }

  @Test
  public void testImpactLineageLifecycleFirstWinsKeepsOneDestination() throws Exception {
    graphService.addEdge(
        operationContext,
        new Edge(
            dataset2Urn,
            dataset1Urn,
            downstreamOf,
            0L,
            null,
            0L,
            null,
            null,
            lifeCycleOwnerOne,
            null));
    graphService.addEdge(
        operationContext,
        new Edge(
            dataset2Urn,
            dataset1Urn,
            downstreamOf,
            0L,
            null,
            0L,
            null,
            null,
            lifeCycleOwnerTwo,
            null));
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            graphService.getLineageRegistry(),
            dataset1Urn.getEntityType(),
            LineageDirection.DOWNSTREAM);
    EntityLineageResult impact =
        graphService.getImpactLineage(operationContext, dataset1Urn, filters, 1);
    long destCount =
        impact.getRelationships().stream().filter(r -> dataset2Urn.equals(r.getEntity())).count();
    assertEquals(destCount, 1);

    EntityLineageResult ui =
        graphService.getLineage(operationContext, dataset1Urn, filters, 0, 100, 1);
    long uiDestCount =
        ui.getRelationships().stream().filter(r -> dataset2Urn.equals(r.getEntity())).count();
    assertEquals(uiDestCount, 2);
  }

  private static LineageRelationship relationship(EntityLineageResult result, Urn entity) {
    return result.getRelationships().stream()
        .filter(r -> entity.equals(r.getEntity()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("missing relationship for " + entity));
  }
}
