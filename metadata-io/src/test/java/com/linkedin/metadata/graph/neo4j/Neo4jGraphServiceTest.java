package com.linkedin.metadata.graph.neo4j;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.graph.GraphServiceTestBaseNoVia;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Neo4jGraphServiceTest extends GraphServiceTestBaseNoVia {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jGraphService _client;
  @Getter private OperationContext operationContext;

  private static final String TAG_RELATIONSHIP = "SchemaFieldTaggedWith";

  @BeforeClass
  public void init() {
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());

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

    _client = new Neo4jGraphService(operationContext, lineageRegistry, _driver);
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
  protected @Nonnull GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() {}

  @Override
  protected void assertEqualsAnyOrder(
      RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    // https://github.com/datahub-project/datahub/issues/3118
    // Neo4jGraphService produces duplicates, which is here ignored until fixed
    // actual.count and actual.total not tested due to duplicates
    assertEquals(actual.getStart(), expected.getStart());
    assertEqualsAnyOrder(actual.getEntities(), expected.getEntities(), RELATED_ENTITY_COMPARATOR);
  }

  @Override
  protected <T> void assertEqualsAnyOrder(
      List<T> actual, List<T> expected, Comparator<T> comparator) {
    // https://github.com/datahub-project/datahub/issues/3118
    // Neo4jGraphService produces duplicates, which is here ignored until fixed
    assertEquals(new HashSet<>(actual), new HashSet<>(expected));
  }

  @Override
  @Test(dataProvider = "NoViaFindRelatedEntitiesSourceTypeTests")
  public void testFindRelatedEntitiesSourceType(
      String datasetType,
      Set<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3119
      throw new SkipException("Neo4jGraphService does not support empty source type");
    }
    if (datasetType != null && datasetType.equals(GraphServiceTestBase.userType)) {
      // https://github.com/datahub-project/datahub/issues/3123
      // only test cases with "user" type fail due to this bug
      throw new SkipException("Neo4jGraphService does not apply source / destination types");
    }
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
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3119
      throw new SkipException("Neo4jGraphService does not support empty destination type");
    }
    if (relationshipTypes.contains(hasOwner)) {
      // https://github.com/datahub-project/datahub/issues/3123
      // only test cases with "HasOwner" relatioship fail due to this bug
      throw new SkipException("Neo4jGraphService does not apply source / destination types");
    }
    super.testFindRelatedEntitiesDestinationType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
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
    throw new SkipException(
        "Neo4jGraphService does not manage to add all edges added concurrently");
  }

  @Test
  public void testRemoveEdge() throws Exception {
    DatasetUrn datasetUrn =
        new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.TEST);
    TagUrn tagUrn = new TagUrn("newTag");
    Edge edge = new Edge(datasetUrn, tagUrn, TAG_RELATIONSHIP, null, null, null, null, null);
    getGraphService().addEdge(edge);

    RelatedEntitiesResult result =
        getGraphService()
            .findRelatedEntities(
                operationContext,
                Set.of(datasetType),
                newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
                Set.of("tag"),
                EMPTY_FILTER,
                Set.of(TAG_RELATIONSHIP),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
                0,
                100);
    assertEquals(result.getTotal(), 1);
    getGraphService().removeEdge(edge);

    result =
        getGraphService()
            .findRelatedEntities(
                operationContext,
                Set.of(datasetType),
                newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
                Set.of("tag"),
                EMPTY_FILTER,
                Set.of(TAG_RELATIONSHIP),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
                0,
                100);
    assertEquals(result.getTotal(), 0);
  }

  private Set<UrnArray> getPathUrnArraysFromLineageResult(EntityLineageResult result) {
    return result.getRelationships().stream()
        .map(x -> x.getPaths().get(0))
        .collect(Collectors.toSet());
  }

  @Test
  public void testGetLineage() {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            // d1 <-Consumes- dj1 -Produces-> d2 <-DownstreamOf- d3 <-DownstreamOf- d5
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 1L, null, 3L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 5L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null),
            new Edge(dataset5Urn, dataset3Urn, downstreamOf, 11L, null, null, null, null),

            // another path between d2 and d5 which is shorter
            // d1 <-DownstreamOf- d4 <-DownstreamOf- d5
            new Edge(dataset4Urn, dataset1Urn, downstreamOf, 13L, null, 13L, null, null),
            new Edge(dataset5Urn, dataset4Urn, downstreamOf, 13L, null, 13L, null, null));
    edges.forEach(service::addEdge);

    // simple path finding
    final var upstreamLineageDataset3Hop3 =
        service.getLineage(operationContext, dataset3Urn, LineageDirection.UPSTREAM, 0, 1000, 3);
    assertEquals(upstreamLineageDataset3Hop3.getTotal().intValue(), 3);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageDataset3Hop3),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn, dataset1Urn)));

    // simple path finding
    final var upstreamLineageDatasetFiveHop2 =
        service.getLineage(operationContext, dataset5Urn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineageDatasetFiveHop2.getTotal().intValue(), 4);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageDatasetFiveHop2),
        Set.of(
            new UrnArray(dataset5Urn, dataset3Urn),
            new UrnArray(dataset5Urn, dataset3Urn, dataset2Urn),
            new UrnArray(dataset5Urn, dataset4Urn),
            new UrnArray(dataset5Urn, dataset4Urn, dataset1Urn)));

    // there are two paths from p5 to p1, one longer and one shorter, and the longer one is
    // discarded from result
    final var upstreamLineageDataset5Hop5 =
        service.getLineage(operationContext, dataset5Urn, LineageDirection.UPSTREAM, 0, 1000, 5);
    assertEquals(upstreamLineageDataset5Hop5.getTotal().intValue(), 5);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageDataset5Hop5),
        Set.of(
            new UrnArray(dataset5Urn, dataset3Urn),
            new UrnArray(dataset5Urn, dataset3Urn, dataset2Urn),
            new UrnArray(dataset5Urn, dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset5Urn, dataset4Urn),
            new UrnArray(dataset5Urn, dataset4Urn, dataset1Urn)));

    // downstream lookup
    final var downstreamLineageDataset1Hop2 =
        service.getLineage(operationContext, dataset1Urn, LineageDirection.DOWNSTREAM, 0, 1000, 2);
    assertEquals(downstreamLineageDataset1Hop2.getTotal().intValue(), 4);
    assertEquals(
        getPathUrnArraysFromLineageResult(downstreamLineageDataset1Hop2),
        Set.of(
            new UrnArray(dataset1Urn, dataJobOneUrn),
            new UrnArray(dataset1Urn, dataJobOneUrn, dataset2Urn),
            new UrnArray(dataset1Urn, dataset4Urn),
            new UrnArray(dataset1Urn, dataset4Urn, dataset5Urn)));
  }

  @Test
  public void testGetLineageTimeFilterQuery() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            // d1 <-Consumes- dj1 -Produces-> d2 <-DownstreamOf- d3 <-DownstreamOf- d4
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 1L, null, 3L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 5L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null),
            new Edge(dataset4Urn, dataset3Urn, downstreamOf, 11L, null, null, null, null));
    edges.forEach(service::addEdge);

    // no time filtering
    EntityLineageResult upstreamLineageTwoHops =
        service.getLineage(operationContext, dataset4Urn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineageTwoHops.getTotal().intValue(), 2);
    assertEquals(upstreamLineageTwoHops.getRelationships().size(), 2);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTwoHops),
        Set.of(
            new UrnArray(dataset4Urn, dataset3Urn),
            new UrnArray(dataset4Urn, dataset3Urn, dataset2Urn)));

    // with time filtering
    EntityLineageResult upstreamLineageTwoHopsWithTimeFilter =
        service.getLineage(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setStartTimeMillis(10L).setEndTimeMillis(12L)),
            dataset4Urn,
            LineageDirection.UPSTREAM,
            0,
            1000,
            2);
    assertEquals(upstreamLineageTwoHopsWithTimeFilter.getTotal().intValue(), 1);
    assertEquals(upstreamLineageTwoHopsWithTimeFilter.getRelationships().size(), 1);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTwoHopsWithTimeFilter),
        Set.of(new UrnArray(dataset4Urn, dataset3Urn)));

    // with time filtering
    EntityLineageResult upstreamLineageTimeFilter =
        service.getLineage(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setStartTimeMillis(2L).setEndTimeMillis(6L)),
            dataset2Urn,
            LineageDirection.UPSTREAM,
            0,
            1000,
            4);
    assertEquals(upstreamLineageTimeFilter.getTotal().intValue(), 2);
    assertEquals(upstreamLineageTimeFilter.getRelationships().size(), 2);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTimeFilter),
        Set.of(
            new UrnArray(dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset2Urn, dataJobOneUrn, dataset1Urn)));

    // with time filtering
    EntityLineageResult downstreamLineageTimeFilter =
        service.getLineage(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setStartTimeMillis(0L).setEndTimeMillis(4L)),
            dataset1Urn,
            LineageDirection.DOWNSTREAM,
            0,
            1000,
            4);
    assertEquals(downstreamLineageTimeFilter.getTotal().intValue(), 1);
    assertEquals(downstreamLineageTimeFilter.getRelationships().size(), 1);
    assertEquals(
        getPathUrnArraysFromLineageResult(downstreamLineageTimeFilter),
        Set.of(new UrnArray(dataset1Urn, dataJobOneUrn)));
  }

  @Test
  public void testGetLineageTimeFilteringSkipsShorterButNonMatchingPaths() {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            // d1 <-Consumes- dj1 -Produces-> d2 <-DownstreamOf- d3
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 5L, null, 5L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 7L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null),

            // d1 <-DownstreamOf- d3 (shorter path from d3 to d1, but with very old time)
            new Edge(dataset3Urn, dataset1Urn, downstreamOf, 1L, null, 2L, null, null));
    edges.forEach(service::addEdge);

    // no time filtering, shorter path from d3 to d1 is returned
    EntityLineageResult upstreamLineageNoTimeFiltering =
        service.getLineage(operationContext, dataset3Urn, LineageDirection.UPSTREAM, 0, 1000, 3);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageNoTimeFiltering),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset3Urn, dataset1Urn)));

    // with time filtering, shorter path from d3 to d1 is excluded so longer path is returned
    EntityLineageResult upstreamLineageTimeFiltering =
        service.getLineage(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setStartTimeMillis(3L).setEndTimeMillis(17L)),
            dataset3Urn,
            LineageDirection.UPSTREAM,
            0,
            1000,
            3);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTimeFiltering),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn, dataset1Urn)));
  }

  @Override
  public void testHighlyConnectedGraphWalk() throws Exception {
    // TODO: explore limit not supported for Neo4J
  }

  @Test
  public void testLineageGraphFiltersImplementation() {
    GraphService service = getGraphService();

    // Create a simple lineage: dataset1 <- dataJob -> dataset2 <- dataset3
    List<Edge> edges =
        Arrays.asList(
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 1L, null, 3L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 5L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null));

    // Add all edges to the graph
    edges.forEach(service::addEdge);

    // Create LineageGraphFilters for upstream lineage
    Set<String> allowedTypes = new HashSet<>(Arrays.asList("dataset", "dataJob"));
    LineageGraphFilters upstreamFilters =
        LineageGraphFilters.withEntityTypes(LineageDirection.UPSTREAM, allowedTypes);

    // Test upstream lineage with the new method signature
    EntityLineageResult upstreamResult =
        service.getLineage(operationContext, dataset3Urn, upstreamFilters, 0, 100, 2);

    // Verify results
    assertEquals(upstreamResult.getTotal().intValue(), 2);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamResult),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn)));

    // Test downstream lineage with the new method signature
    LineageGraphFilters downstreamFilters =
        LineageGraphFilters.withEntityTypes(LineageDirection.DOWNSTREAM, allowedTypes);

    EntityLineageResult downstreamResult =
        service.getLineage(operationContext, dataset1Urn, downstreamFilters, 0, 100, 2);

    // Verify results
    assertEquals(downstreamResult.getTotal().intValue(), 2);
    assertEquals(
        getPathUrnArraysFromLineageResult(downstreamResult),
        Set.of(
            new UrnArray(dataset1Urn, dataJobOneUrn),
            new UrnArray(dataset1Urn, dataJobOneUrn, dataset2Urn)));
  }

  /**
   * Tests that the getPathFindingLabelFilter method correctly handles Set<String> parameter instead
   * of List<String>
   */
  @Test
  public void testGetPathFindingLabelFilterWithSet() throws Exception {
    Neo4jGraphService service = (Neo4jGraphService) getGraphService();

    // Create a reflection method to access the private method
    java.lang.reflect.Method method =
        Neo4jGraphService.class.getDeclaredMethod("getPathFindingLabelFilter", Set.class);
    method.setAccessible(true);

    // Test with single entity type
    Set<String> singleType = Collections.singleton("dataset");
    String singleResult = (String) method.invoke(service, singleType);
    assertEquals(singleResult, "+dataset");

    // Test with multiple entity types
    Set<String> multipleTypes = new HashSet<>(Arrays.asList("dataset", "dataJob", "container"));
    String multipleResult = (String) method.invoke(service, multipleTypes);

    // Since the order is not guaranteed in a set, verify that all expected parts are present
    assertTrue(multipleResult.contains("+dataset"));
    assertTrue(multipleResult.contains("+dataJob"));
    assertTrue(multipleResult.contains("+container"));
    assertEquals(multipleResult.split("\\|").length, 3);
  }

  /**
   * Tests that the getPathFindingRelationshipFilter method correctly handles Set<String> parameter
   * instead of List<String>
   */
  @Test
  public void testGetPathFindingRelationshipFilterWithSet() throws Exception {
    Neo4jGraphService service = (Neo4jGraphService) getGraphService();

    // Create a reflection method to access the private method
    java.lang.reflect.Method method =
        Neo4jGraphService.class.getDeclaredMethod(
            "getPathFindingRelationshipFilter", Set.class, LineageDirection.class);
    method.setAccessible(true);

    // Test with dataset type and upstream direction
    Set<String> datasetType = Collections.singleton("dataset");
    String upstreamResult = (String) method.invoke(service, datasetType, LineageDirection.UPSTREAM);

    // Verify the result is not empty
    assertFalse(upstreamResult.isEmpty());

    // Test with null direction which should return relationships without direction specifiers
    String allDirectionsResult = (String) method.invoke(service, datasetType, null);

    // Verify the result is not empty and contains expected format
    assertFalse(allDirectionsResult.isEmpty());

    // With null direction, the result should not contain '<' or '>' characters
    // which would indicate direction-specific relationships
    assertFalse(allDirectionsResult.contains("<"));
    assertFalse(allDirectionsResult.contains(">"));
  }

  /**
   * Tests the generateLineageStatementAndParameters method with LineageGraphFilters parameter
   * instead of separate LineageDirection and GraphFilters parameters
   */
  @Test
  public void testGenerateLineageStatementAndParameters() throws Exception {
    Neo4jGraphService service = (Neo4jGraphService) getGraphService();

    // Create a reflection method to access the private method
    java.lang.reflect.Method method =
        Neo4jGraphService.class.getDeclaredMethod(
            "generateLineageStatementAndParameters",
            Urn.class,
            LineageGraphFilters.class,
            int.class,
            LineageFlags.class);
    method.setAccessible(true);

    // Create LineageGraphFilters
    Set<String> allowedTypes = new HashSet<>(Arrays.asList("dataset", "dataJob"));
    LineageGraphFilters upstreamFilters =
        LineageGraphFilters.withEntityTypes(LineageDirection.UPSTREAM, allowedTypes);

    // Test with no time filters
    Object result = method.invoke(service, dataset1Urn, upstreamFilters, 2, null);

    // Result should be a Pair<String, Map<String, Object>>
    assertNotNull(result);

    // Test we can access the first element of the pair (the query string)
    java.lang.reflect.Method firstMethod = result.getClass().getDeclaredMethod("getFirst");
    firstMethod.setAccessible(true);
    String query = (String) firstMethod.invoke(result);

    // Verify the query contains expected structure
    assertNotNull(query);
    assertTrue(query.contains("MATCH"));
    assertTrue(query.contains("CALL apoc.path"));

    // Test with time filters
    LineageFlags timeFlags = new LineageFlags().setStartTimeMillis(100L).setEndTimeMillis(200L);

    Object resultWithTime = method.invoke(service, dataset1Urn, upstreamFilters, 2, timeFlags);
    assertNotNull(resultWithTime);

    // Get the query string
    String timeFilteredQuery = (String) firstMethod.invoke(resultWithTime);

    // Verify the query contains the time filtering logic
    assertNotNull(timeFilteredQuery);
    assertTrue(timeFilteredQuery.contains("$startTimeMillis"));
    assertTrue(timeFilteredQuery.contains("$endTimeMillis"));
  }

  /**
   * Tests that the computeEntityTypeWhereClause method correctly handles Set<String> parameters
   * instead of List<String> parameters
   */
  @Test
  public void testComputeEntityTypeWhereClause() throws Exception {
    Neo4jGraphService service = (Neo4jGraphService) getGraphService();

    // Create a reflection method to access the private method
    java.lang.reflect.Method method =
        Neo4jGraphService.class.getDeclaredMethod(
            "computeEntityTypeWhereClause", Set.class, Set.class);
    method.setAccessible(true);

    // Test with empty source and destination types
    Set<String> emptySet = Collections.emptySet();
    String emptyResult = (String) method.invoke(service, emptySet, emptySet);
    assertEquals(emptyResult, " WHERE left(type(r), 2)<>'r_' ");

    // Test with only source types
    Set<String> sourceTypes = new HashSet<>(Arrays.asList("dataset", "dataJob"));
    String sourceOnlyResult = (String) method.invoke(service, sourceTypes, emptySet);

    // Verify the result contains the source type conditions
    assertTrue(sourceOnlyResult.contains("src:dataset"));
    assertTrue(sourceOnlyResult.contains("src:dataJob"));
    assertTrue(sourceOnlyResult.contains(" OR "));

    // Test with only destination types
    Set<String> destTypes = new HashSet<>(Arrays.asList("user", "tag"));
    String destOnlyResult = (String) method.invoke(service, emptySet, destTypes);

    // Verify the result contains the destination type conditions
    assertTrue(destOnlyResult.contains("dest:user"));
    assertTrue(destOnlyResult.contains("dest:tag"));
    assertTrue(destOnlyResult.contains(" OR "));

    // Test with both source and destination types
    String bothResult = (String) method.invoke(service, sourceTypes, destTypes);

    // Verify the result contains both the source and destination type conditions
    assertTrue(bothResult.contains("src:dataset"));
    assertTrue(bothResult.contains("src:dataJob"));
    assertTrue(bothResult.contains("dest:user"));
    assertTrue(bothResult.contains("dest:tag"));
    assertTrue(bothResult.contains(" AND "));
  }

  /** Test the GraphFilters parameter in findRelatedEntities method */
  @Test
  public void testFindRelatedEntitiesWithGraphFilters() throws Exception {
    DatasetUrn datasetUrn =
        new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.TEST);
    TagUrn tagUrn = new TagUrn("newTag");
    Edge edge = new Edge(datasetUrn, tagUrn, TAG_RELATIONSHIP, null, null, null, null, null);
    getGraphService().addEdge(edge);

    // Create GraphFilters
    Set<String> sourceTypes = Set.of(datasetType);
    Filter sourceFilter = newFilter(Collections.singletonMap("urn", datasetUrn.toString()));
    Set<String> destTypes = Set.of("tag");
    Filter destFilter = EMPTY_FILTER;
    Set<String> relationshipTypes = Set.of(TAG_RELATIONSHIP);
    RelationshipFilter relationshipFilter =
        newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING);

    GraphFilters graphFilters =
        new GraphFilters(
            sourceFilter,
            destFilter,
            sourceTypes,
            destTypes,
            relationshipTypes,
            relationshipFilter);

    // Query with GraphFilters
    RelatedEntitiesResult result =
        ((Neo4jGraphService) getGraphService())
            .findRelatedEntities(operationContext, graphFilters, 0, 100);

    assertEquals(result.getTotal(), 1);
    assertEquals(result.getEntities().get(0).getRelationshipType(), TAG_RELATIONSHIP);
  }

  /** Test the removeEdgesFromNode method with Set<String> parameter instead of List<String> */
  @Test
  public void testRemoveEdgesFromNodeWithSetParameter() throws Exception {
    DatasetUrn datasetUrn =
        new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.TEST);
    TagUrn tagUrn = new TagUrn("newTag");
    Edge edge = new Edge(datasetUrn, tagUrn, TAG_RELATIONSHIP, null, null, null, null, null);
    getGraphService().addEdge(edge);

    // Verify edge exists
    GraphFilters graphFilters =
        new GraphFilters(
            newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
            EMPTY_FILTER,
            Set.of(datasetType),
            Set.of("tag"),
            Set.of(TAG_RELATIONSHIP),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING));

    RelatedEntitiesResult resultBefore =
        ((Neo4jGraphService) getGraphService())
            .findRelatedEntities(operationContext, graphFilters, 0, 100);
    assertEquals(resultBefore.getTotal(), 1);

    // Now remove edges using Set<String> parameter
    Set<String> relationshipTypes = Set.of(TAG_RELATIONSHIP);
    RelationshipFilter relationshipFilter =
        newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING);

    ((Neo4jGraphService) getGraphService())
        .removeEdgesFromNode(operationContext, datasetUrn, relationshipTypes, relationshipFilter);

    // Verify edge is removed
    RelatedEntitiesResult resultAfter =
        ((Neo4jGraphService) getGraphService())
            .findRelatedEntities(operationContext, graphFilters, 0, 100);
    assertEquals(resultAfter.getTotal(), 0);
  }
}
