package com.linkedin.metadata.graph;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static io.datahubproject.test.search.SearchTestUtils.getGraphQueryConfiguration;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.dgraph.DgraphGraphService;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Base class for testing any GraphService implementation. Derive the test class from this base and
 * get your GraphService implementation tested with all these tests.
 *
 * <p>You can add implementation specific tests in derived classes, or add general tests here and
 * have all existing implementations tested in the same way.
 *
 * <p>The `getPopulatedGraphService` method calls `GraphService.addEdge` to provide a populated
 * Graph. Feel free to add a test to your test implementation that calls `getPopulatedGraphService`
 * and asserts the state of the graph in an implementation specific way.
 */
public abstract class GraphServiceTestBase extends AbstractTestNGSpringContextTests {

  protected static class RelatedEntityComparator implements Comparator<RelatedEntity> {
    @Override
    public int compare(RelatedEntity left, RelatedEntity right) {
      int cmp = left.getRelationshipType().compareTo(right.getRelationshipType());
      if (cmp != 0) {
        return cmp;
      }
      return left.getUrn().compareTo(right.getUrn());
    }
  }

  protected static final RelatedEntityComparator RELATED_ENTITY_COMPARATOR =
      new RelatedEntityComparator();

  /** Some test URN types. */
  protected static String datasetType = "dataset";

  protected static String userType = "user";

  /** Some test datasets. */
  protected static String dataset1UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset1,PROD)";

  protected static String dataset2UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset2,PROD)";
  protected static String dataset3UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset3,PROD)";
  protected static String dataset4UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset4,PROD)";
  protected static String dataset5UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset5,PROD)";

  protected static String dataset6UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset6,PROD)";

  protected static String dataset7UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset7,PROD)";

  protected static String dataset8UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset8,PROD)";
  protected static String dataset9UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset9,PROD)";
  protected static String dataset10UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset10,PROD)";
  protected static String dataset11UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset11,PROD)";
  protected static String dataset12UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset12,PROD)";
  protected static String dataset13UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset13,PROD)";
  protected static String dataset14UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset14,PROD)";
  protected static String dataset15UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset15,PROD)";
  protected static String dataset16UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset16,PROD)";
  protected static String dataset17UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset17,PROD)";
  protected static String dataset18UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset18,PROD)";

  protected static String dataset19UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset19,PROD)";

  protected static String dataset20UrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDataset20,PROD)";

  protected static Urn dataset1Urn = createFromString(dataset1UrnString);
  protected static Urn dataset2Urn = createFromString(dataset2UrnString);
  protected static Urn dataset3Urn = createFromString(dataset3UrnString);
  protected static Urn dataset4Urn = createFromString(dataset4UrnString);
  protected static Urn dataset5Urn = createFromString(dataset5UrnString);
  protected static Urn dataset6Urn = createFromString(dataset6UrnString);

  protected static Urn dataset7Urn = createFromString(dataset7UrnString);

  protected static Urn dataset8Urn = createFromString(dataset8UrnString);
  protected static Urn dataset9Urn = createFromString(dataset9UrnString);
  protected static Urn dataset10Urn = createFromString(dataset10UrnString);
  protected static Urn dataset11Urn = createFromString(dataset11UrnString);
  protected static Urn dataset12Urn = createFromString(dataset12UrnString);
  protected static Urn dataset13Urn = createFromString(dataset13UrnString);
  protected static Urn dataset14Urn = createFromString(dataset14UrnString);
  protected static Urn dataset15Urn = createFromString(dataset15UrnString);
  protected static Urn dataset16Urn = createFromString(dataset16UrnString);
  protected static Urn dataset17Urn = createFromString(dataset17UrnString);
  protected static Urn dataset18Urn = createFromString(dataset18UrnString);

  protected static Urn dataset19Urn = createFromString(dataset19UrnString);

  protected static Urn dataset20Urn = createFromString(dataset20UrnString);
  protected static List<Urn> datasetUrns =
      List.of(
          dataset1Urn,
          dataset2Urn,
          dataset3Urn,
          dataset4Urn,
          dataset5Urn,
          dataset6Urn,
          dataset7Urn,
          dataset8Urn,
          dataset9Urn,
          dataset10Urn,
          dataset11Urn,
          dataset12Urn,
          dataset13Urn,
          dataset14Urn,
          dataset15Urn,
          dataset16Urn,
          dataset17Urn,
          dataset18Urn,
          dataset19Urn,
          dataset20Urn);

  protected static final String schemaFieldUrn1String =
      "urn:li:schemaField:(" + dataset5UrnString + ",fieldOne)";
  protected static final String schemaFieldUrn2String =
      "urn:li:schemaField:(" + dataset4UrnString + ",fieldTwo)";

  protected static final String lifeCycleOwner1String =
      "urn:li:dataJob:(urn:li:dataFlow:(fivetran,calendar_elected,PROD),calendar_elected)";
  protected static final String lifeCycleOwner2String =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";

  protected static final Urn schemaFieldUrnOne = createFromString(schemaFieldUrn1String);
  protected static final Urn schemaFieldUrnTwo = createFromString(schemaFieldUrn2String);
  protected static final Urn lifeCycleOwnerOne = createFromString(lifeCycleOwner1String);
  protected static final Urn lifeCycleOwnerTwo = createFromString(lifeCycleOwner2String);

  protected static String unknownUrnString = "urn:li:unknown:(urn:li:unknown:Unknown)";

  /** Some dataset owners. */
  protected static String userOneUrnString =
      "urn:li:" + userType + ":(urn:li:user:system,Ingress,PROD)";

  protected static String userTwoUrnString =
      "urn:li:" + userType + ":(urn:li:user:individual,UserA,DEV)";

  protected static Urn userOneUrn = createFromString(userOneUrnString);
  protected static Urn userTwoUrn = createFromString(userTwoUrnString);

  protected static Urn unknownUrn = createFromString(unknownUrnString);

  /** Some data jobs */
  protected static Urn dataJobOneUrn =
      new DataJobUrn(new DataFlowUrn("orchestrator", "flow", "cluster"), "job1");

  protected static Urn dataJobTwoUrn =
      new DataJobUrn(new DataFlowUrn("orchestrator", "flow", "cluster"), "job2");

  /** Some test relationships. */
  protected static String downstreamOf = "DownstreamOf";

  protected static String hasOwner = "HasOwner";
  protected static String knowsUser = "KnowsUser";
  protected static String produces = "Produces";
  protected static String consumes = "Consumes";
  protected static Set<String> allRelationshipTypes =
      new HashSet<>(Arrays.asList(downstreamOf, hasOwner, knowsUser));

  /** Some expected related entities. */
  protected static RelatedEntity downstreamOfDatasetOneRelatedEntity =
      new RelatedEntity(downstreamOf, dataset1UrnString);

  protected static RelatedEntity downstreamOfDatasetTwoRelatedEntity =
      new RelatedEntity(downstreamOf, dataset2UrnString);
  protected static RelatedEntity downstreamOfDatasetThreeRelatedEntity =
      new RelatedEntity(downstreamOf, dataset3UrnString);
  protected static RelatedEntity downstreamOfDatasetFourRelatedEntity =
      new RelatedEntity(downstreamOf, dataset4UrnString);
  protected static final RelatedEntity downstreamOfSchemaFieldOneVia =
      new RelatedEntity(downstreamOf, schemaFieldUrn1String, lifeCycleOwner1String);
  protected static final RelatedEntity downstreamOfSchemaFieldOne =
      new RelatedEntity(downstreamOf, schemaFieldUrn1String);
  protected static final RelatedEntity downstreamOfSchemaFieldTwoVia =
      new RelatedEntity(downstreamOf, schemaFieldUrn2String, lifeCycleOwner1String);
  protected static final RelatedEntity downstreamOfSchemaFieldTwo =
      new RelatedEntity(downstreamOf, schemaFieldUrn2String);

  protected static RelatedEntity hasOwnerDatasetOneRelatedEntity =
      new RelatedEntity(hasOwner, dataset1UrnString);
  protected static RelatedEntity hasOwnerDatasetTwoRelatedEntity =
      new RelatedEntity(hasOwner, dataset2UrnString);
  protected static RelatedEntity hasOwnerDatasetThreeRelatedEntity =
      new RelatedEntity(hasOwner, dataset3UrnString);
  protected static RelatedEntity hasOwnerDatasetFourRelatedEntity =
      new RelatedEntity(hasOwner, dataset4UrnString);
  protected static RelatedEntity hasOwnerUserOneRelatedEntity =
      new RelatedEntity(hasOwner, userOneUrnString);
  protected static RelatedEntity hasOwnerUserTwoRelatedEntity =
      new RelatedEntity(hasOwner, userTwoUrnString);

  protected static RelatedEntity knowsUserOneRelatedEntity =
      new RelatedEntity(knowsUser, userOneUrnString);
  protected static RelatedEntity knowsUserTwoRelatedEntity =
      new RelatedEntity(knowsUser, userTwoUrnString);

  /** Some relationship filters. */
  protected static RelationshipFilter outgoingRelationships =
      newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING);

  protected static RelationshipFilter incomingRelationships =
      newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING);
  protected static RelationshipFilter undirectedRelationships =
      newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.UNDIRECTED);

  /** Any source and destination type value. */
  protected static @Nullable List<String> anyType = null;

  protected static final GraphQueryConfiguration _graphQueryConfiguration =
      getGraphQueryConfiguration();
  protected static final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  /** Timeout used to test concurrent ops in doTestConcurrentOp. */
  protected Duration getTestConcurrentOpTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testStaticUrns() {
    assertNotNull(dataset1Urn);
    assertNotNull(dataset2Urn);
    assertNotNull(dataset3Urn);
    assertNotNull(dataset4Urn);

    assertNotNull(userOneUrn);
    assertNotNull(userTwoUrn);
  }

  /**
   * Provides the current GraphService instance to test. This is being called by the test method at
   * most once. The serviced graph should be empty.
   *
   * @return the GraphService instance to test
   * @throws Exception on failure
   */
  @Nonnull
  protected abstract GraphService getGraphService() throws Exception;

  /**
   * Graph services that support multi-path search should override this method to provide a
   * multi-path search enabled GraphService instance.
   *
   * @param enableMultiPathSearch sets multipath search as enabled for the graph service, defaults
   *     to doing nothing unless overridden
   * @return the configured graph service
   * @throws Exception on failure
   */
  @Nonnull
  protected GraphService getGraphService(boolean enableMultiPathSearch) throws Exception {
    return getGraphService();
  }

  /**
   * Allows the specific GraphService test implementation to wait for GraphService writes to be
   * synced / become available to reads.
   *
   * @throws Exception on failure
   */
  protected abstract void syncAfterWrite() throws Exception;

  /**
   * Calls getGraphService to retrieve the test GraphService and populates it with edges via
   * `GraphService.addEdge`.
   *
   * @return test GraphService
   * @throws Exception on failure
   */
  protected GraphService getPopulatedGraphService() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            new Edge(dataset2Urn, dataset1Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset4Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset1Urn, userOneUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset2Urn, userOneUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset3Urn, userTwoUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset4Urn, userTwoUrn, hasOwner, null, null, null, null, null),
            new Edge(userOneUrn, userTwoUrn, knowsUser, null, null, null, null, null),
            new Edge(userTwoUrn, userOneUrn, knowsUser, null, null, null, null, null),
            new Edge(
                schemaFieldUrnOne,
                schemaFieldUrnTwo,
                downstreamOf,
                0L,
                null,
                0L,
                null,
                null,
                lifeCycleOwnerOne,
                lifeCycleOwnerOne),
            new Edge(
                schemaFieldUrnOne,
                schemaFieldUrnTwo,
                downstreamOf,
                0L,
                null,
                0L,
                null,
                null,
                lifeCycleOwnerTwo,
                null));

    edges.forEach(service::addEdge);
    syncAfterWrite();

    return service;
  }

  protected GraphService getLineagePopulatedGraphService() throws Exception {
    return getLineagePopulatedGraphService(_graphQueryConfiguration.isEnableMultiPathSearch());
  }

  protected GraphService getLineagePopulatedGraphService(boolean multiPathSearch) throws Exception {
    GraphService service = getGraphService(multiPathSearch);

    List<Edge> edges =
        Arrays.asList(
            new Edge(dataset2Urn, dataset1Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset4Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset1Urn, userOneUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset2Urn, userOneUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset3Urn, userTwoUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset4Urn, userTwoUrn, hasOwner, null, null, null, null, null),
            new Edge(userOneUrn, userTwoUrn, knowsUser, null, null, null, null, null),
            new Edge(userTwoUrn, userOneUrn, knowsUser, null, null, null, null, null),
            new Edge(dataJobOneUrn, dataset1Urn, consumes, null, null, null, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, consumes, null, null, null, null, null),
            new Edge(dataJobOneUrn, dataset3Urn, produces, null, null, null, null, null),
            new Edge(dataJobOneUrn, dataset4Urn, produces, null, null, null, null, null),
            new Edge(dataJobTwoUrn, dataset1Urn, consumes, null, null, null, null, null),
            new Edge(dataJobTwoUrn, dataset2Urn, consumes, null, null, null, null, null),
            new Edge(dataJobTwoUrn, dataJobOneUrn, downstreamOf, null, null, null, null, null));

    edges.forEach(service::addEdge);
    syncAfterWrite();

    return service;
  }

  protected static @Nullable Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  protected void assertEqualsAnyOrder(RelatedEntitiesResult actual, List<RelatedEntity> expected) {
    assertEqualsAnyOrder(
        actual, new RelatedEntitiesResult(0, expected.size(), expected.size(), expected));
  }

  protected void assertEqualsAnyOrder(
      RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    assertEquals(actual.start, expected.start);
    assertEquals(actual.count, expected.count);
    assertEquals(actual.total, expected.total);
    assertEqualsAnyOrder(actual.entities, expected.entities, RELATED_ENTITY_COMPARATOR);
  }

  protected <T> void assertEqualsAnyOrder(List<T> actual, List<T> expected) {
    assertEquals(
        actual.stream().sorted().collect(Collectors.toList()),
        expected.stream().sorted().collect(Collectors.toList()));
  }

  protected <T> void assertEqualsAnyOrder(
      List<T> actual, List<T> expected, Comparator<T> comparator) {
    assertEquals(
        actual.stream().sorted(comparator).collect(Collectors.toList()),
        expected.stream().sorted(comparator).collect(Collectors.toList()));
  }

  @DataProvider(name = "AddEdgeTests")
  public Object[][] getAddEdgeTests() {
    return new Object[][] {
      new Object[] {Collections.emptyList(), Collections.emptyList(), Collections.emptyList()},
      new Object[] {
        Collections.singletonList(
            new Edge(dataset1Urn, dataset2Urn, downstreamOf, null, null, null, null, null)),
        Collections.singletonList(downstreamOfDatasetTwoRelatedEntity),
        Collections.singletonList(downstreamOfDatasetOneRelatedEntity)
      },
      new Object[] {
        Arrays.asList(
            new Edge(dataset1Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset2Urn, dataset3Urn, downstreamOf, null, null, null, null, null)),
        Arrays.asList(downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity),
        Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        Arrays.asList(
            new Edge(dataset1Urn, dataset2Urn, downstreamOf, null, null, null, null, null),
            new Edge(dataset1Urn, userOneUrn, hasOwner, null, null, null, null, null),
            new Edge(dataset2Urn, userTwoUrn, hasOwner, null, null, null, null, null),
            new Edge(userOneUrn, userTwoUrn, knowsUser, null, null, null, null, null)),
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            hasOwnerUserOneRelatedEntity,
            hasOwnerUserTwoRelatedEntity,
            knowsUserTwoRelatedEntity),
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            knowsUserOneRelatedEntity)
      },
      new Object[] {
        Arrays.asList(
            new Edge(userOneUrn, userOneUrn, knowsUser, null, null, null, null, null),
            new Edge(userOneUrn, userOneUrn, knowsUser, null, null, null, null, null),
            new Edge(userOneUrn, userOneUrn, knowsUser, null, null, null, null, null)),
        Collections.singletonList(knowsUserOneRelatedEntity),
        Collections.singletonList(knowsUserOneRelatedEntity)
      }
    };
  }

  @Test(dataProvider = "AddEdgeTests")
  public void testAddEdge(
      List<Edge> edges, List<RelatedEntity> expectedOutgoing, List<RelatedEntity> expectedIncoming)
      throws Exception {
    GraphService service = getGraphService();

    edges.forEach(service::addEdge);
    syncAfterWrite();

    RelatedEntitiesResult relatedOutgoing =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(relatedOutgoing, expectedOutgoing);

    RelatedEntitiesResult relatedIncoming =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            incomingRelationships,
            0,
            100);
    assertEqualsAnyOrder(relatedIncoming, expectedIncoming);
  }

  @Test
  public void testPopulatedGraphService() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedOutgoingEntitiesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    // All downstreamOf, hasOwner, or knowsUser relationships, outgoing
    assertEqualsAnyOrder(
        relatedOutgoingEntitiesBeforeRemove,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity,
            knowsUserOneRelatedEntity, knowsUserTwoRelatedEntity,
            downstreamOfSchemaFieldTwoVia, downstreamOfSchemaFieldTwo));
    RelatedEntitiesResult relatedIncomingEntitiesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            incomingRelationships,
            0,
            100);
    // All downstreamOf, hasOwner, or knowsUser relationships, incoming
    assertEqualsAnyOrder(
        relatedIncomingEntitiesBeforeRemove,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity,
            hasOwnerDatasetFourRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            downstreamOfSchemaFieldOneVia,
            downstreamOfSchemaFieldOne));
    EntityLineageResult viaNodeResult =
        service.getLineage(
            operationContext,
            schemaFieldUrnOne,
            LineageDirection.UPSTREAM,
            new GraphFilters(List.of("schemaField")),
            0,
            1000,
            100);
    // Multi-path enabled
    assertEquals(viaNodeResult.getRelationships().size(), 2);
    // First one is via node
    assertTrue(
        viaNodeResult.getRelationships().get(0).getPaths().get(0).contains(lifeCycleOwnerOne));
    EntityLineageResult viaNodeResultNoMulti =
        getGraphService(false)
            .getLineage(
                operationContext,
                schemaFieldUrnOne,
                LineageDirection.UPSTREAM,
                new GraphFilters(List.of("schemaField")),
                0,
                1000,
                100);

    // Multi-path disabled, still has two because via flow creates both edges in response
    assertEquals(viaNodeResultNoMulti.getRelationships().size(), 2);
    // First one is via node
    assertTrue(
        viaNodeResult.getRelationships().get(0).getPaths().get(0).contains(lifeCycleOwnerOne));

    // reset graph service
    getGraphService();
  }

  @Test
  public void testPopulatedGraphServiceGetLineage() throws Exception {
    GraphService service = getLineagePopulatedGraphService();

    EntityLineageResult upstreamLineage =
        service.getLineage(operationContext, dataset1Urn, LineageDirection.UPSTREAM, 0, 1000, 1);
    assertEquals(upstreamLineage.getTotal().intValue(), 0);
    assertEquals(upstreamLineage.getRelationships().size(), 0);

    EntityLineageResult downstreamLineage =
        service.getLineage(operationContext, dataset1Urn, LineageDirection.DOWNSTREAM, 0, 1000, 1);
    assertEquals(downstreamLineage.getTotal().intValue(), 3);
    assertEquals(downstreamLineage.getRelationships().size(), 3);
    Map<Urn, LineageRelationship> relationships =
        downstreamLineage.getRelationships().stream()
            .collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    assertTrue(relationships.containsKey(dataset2Urn));
    assertEquals(relationships.get(dataset2Urn).getType(), downstreamOf);
    assertTrue(relationships.containsKey(dataJobOneUrn));
    assertEquals(relationships.get(dataJobOneUrn).getType(), consumes);
    assertTrue(relationships.containsKey(dataJobTwoUrn));
    assertEquals(relationships.get(dataJobTwoUrn).getType(), consumes);

    upstreamLineage =
        service.getLineage(operationContext, dataset3Urn, LineageDirection.UPSTREAM, 0, 1000, 1);
    assertEquals(upstreamLineage.getTotal().intValue(), 2);
    assertEquals(upstreamLineage.getRelationships().size(), 2);
    relationships =
        upstreamLineage.getRelationships().stream()
            .collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    assertTrue(relationships.containsKey(dataset2Urn));
    assertEquals(relationships.get(dataset2Urn).getType(), downstreamOf);
    assertTrue(relationships.containsKey(dataJobOneUrn));
    assertEquals(relationships.get(dataJobOneUrn).getType(), produces);

    downstreamLineage =
        service.getLineage(operationContext, dataset3Urn, LineageDirection.DOWNSTREAM, 0, 1000, 1);
    assertEquals(downstreamLineage.getTotal().intValue(), 0);
    assertEquals(downstreamLineage.getRelationships().size(), 0);

    upstreamLineage =
        service.getLineage(operationContext, dataJobOneUrn, LineageDirection.UPSTREAM, 0, 1000, 1);
    assertEquals(upstreamLineage.getTotal().intValue(), 2);
    assertEquals(upstreamLineage.getRelationships().size(), 2);
    relationships =
        upstreamLineage.getRelationships().stream()
            .collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    assertTrue(relationships.containsKey(dataset1Urn));
    assertEquals(relationships.get(dataset1Urn).getType(), consumes);
    assertTrue(relationships.containsKey(dataset2Urn));
    assertEquals(relationships.get(dataset2Urn).getType(), consumes);

    downstreamLineage =
        service.getLineage(
            operationContext, dataJobOneUrn, LineageDirection.DOWNSTREAM, 0, 1000, 1);
    assertEquals(downstreamLineage.getTotal().intValue(), 3);
    assertEquals(downstreamLineage.getRelationships().size(), 3);
    relationships =
        downstreamLineage.getRelationships().stream()
            .collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    assertTrue(relationships.containsKey(dataset3Urn));
    assertEquals(relationships.get(dataset3Urn).getType(), produces);
    assertTrue(relationships.containsKey(dataset4Urn));
    assertEquals(relationships.get(dataset4Urn).getType(), produces);
    assertTrue(relationships.containsKey(dataJobTwoUrn));
    assertEquals(relationships.get(dataJobTwoUrn).getType(), downstreamOf);
  }

  @DataProvider(name = "FindRelatedEntitiesSourceEntityFilterTests")
  public Object[][] getFindRelatedEntitiesSourceEntityFilterTests() {
    return new Object[][] {
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Collections.singletonList(downstreamOfDatasetOneRelatedEntity)
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(hasOwner),
        outgoingRelationships,
        Collections.singletonList(hasOwnerUserOneRelatedEntity)
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(hasOwner),
        incomingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(hasOwner),
        undirectedRelationships,
        Collections.singletonList(hasOwnerUserOneRelatedEntity)
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(hasOwner),
        outgoingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(hasOwner),
        incomingRelationships,
        Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity)
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(hasOwner),
        undirectedRelationships,
        Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity)
      }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesSourceEntityFilterTests")
  public void testFindRelatedEntitiesSourceEntityFilter(
      Filter sourceEntityFilter,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    doTestFindRelatedEntities(
        sourceEntityFilter,
        EMPTY_FILTER,
        relationshipTypes,
        relationships,
        expectedRelatedEntities);
  }

  @DataProvider(name = "FindRelatedEntitiesDestinationEntityFilterTests")
  public Object[][] getFindRelatedEntitiesDestinationEntityFilterTests() {
    return new Object[][] {
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Collections.singletonList(downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Collections.singletonList(downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        newFilter("urn", dataset2UrnString),
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Collections.singletonList(downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Collections.emptyList()
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(hasOwner),
        outgoingRelationships,
        Collections.singletonList(hasOwnerUserOneRelatedEntity)
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(hasOwner),
        incomingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        newFilter("urn", userOneUrnString),
        Collections.singletonList(hasOwner),
        undirectedRelationships,
        Collections.singletonList(hasOwnerUserOneRelatedEntity)
      }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesDestinationEntityFilterTests")
  public void testFindRelatedEntitiesDestinationEntityFilter(
      Filter destinationEntityFilter,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    doTestFindRelatedEntities(
        EMPTY_FILTER,
        destinationEntityFilter,
        relationshipTypes,
        relationships,
        expectedRelatedEntities);
  }

  private void doTestFindRelatedEntities(
      final Filter sourceEntityFilter,
      final Filter destinationEntityFilter,
      List<String> relationshipTypes,
      final RelationshipFilter relationshipFilter,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            anyType,
            sourceEntityFilter,
            anyType,
            destinationEntityFilter,
            relationshipTypes,
            relationshipFilter,
            0,
            10);

    assertEqualsAnyOrder(relatedEntities, expectedRelatedEntities);
  }

  @DataProvider(name = "FindRelatedEntitiesSourceTypeTests")
  public Object[][] getFindRelatedEntitiesSourceTypeTests() {
    return new Object[][] {
      // All DownstreamOf relationships, outgoing
      new Object[] {
        null,
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfSchemaFieldTwoVia,
            downstreamOfSchemaFieldTwo)
      },
      // All DownstreamOf relationships, incoming
      new Object[] {
        null,
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            downstreamOfSchemaFieldOneVia,
            downstreamOfSchemaFieldOne)
      },
      // All DownstreamOf relationships, both directions
      new Object[] {
        null,
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity,
            downstreamOfSchemaFieldTwoVia, downstreamOfSchemaFieldTwo,
            downstreamOfSchemaFieldOneVia, downstreamOfSchemaFieldOne)
      },

      // "" used to be any type before v0.9.0, which is now encoded by null
      new Object[] {
        "", Collections.singletonList(downstreamOf), outgoingRelationships, Collections.emptyList()
      },
      new Object[] {
        "", Collections.singletonList(downstreamOf), incomingRelationships, Collections.emptyList()
      },
      new Object[] {
        "",
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Collections.emptyList()
      },
      new Object[] {
        datasetType,
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        datasetType,
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        userType,
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        userType,
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        userType,
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Collections.emptyList()
      },
      new Object[] {
        userType,
        Collections.singletonList(hasOwner),
        outgoingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        userType,
        Collections.singletonList(hasOwner),
        incomingRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      },
      new Object[] {
        userType,
        Collections.singletonList(hasOwner),
        undirectedRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesSourceTypeTests")
  public void testFindRelatedEntitiesSourceType(
      String entityTypeFilter,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    doTestFindRelatedEntities(
        entityTypeFilter != null ? ImmutableList.of(entityTypeFilter) : null,
        anyType,
        relationshipTypes,
        relationships,
        expectedRelatedEntities);
  }

  @DataProvider(name = "FindRelatedEntitiesDestinationTypeTests")
  public Object[][] getFindRelatedEntitiesDestinationTypeTests() {
    return new Object[][] {
      new Object[] {
        null,
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        // All DownstreamOf relationships, outgoing
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfSchemaFieldTwoVia,
            downstreamOfSchemaFieldTwo)
      },
      new Object[] {
        null,
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        // All DownstreamOf relationships, incoming
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            downstreamOfSchemaFieldOneVia,
            downstreamOfSchemaFieldOne)
      },
      new Object[] {
        null,
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity,
            downstreamOfSchemaFieldOneVia, downstreamOfSchemaFieldOne,
            downstreamOfSchemaFieldTwoVia, downstreamOfSchemaFieldTwo)
      },
      new Object[] {
        "", Collections.singletonList(downstreamOf), outgoingRelationships, Collections.emptyList()
      },
      new Object[] {
        "", Collections.singletonList(downstreamOf), incomingRelationships, Collections.emptyList()
      },
      new Object[] {
        "",
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Collections.emptyList()
      },
      new Object[] {
        datasetType,
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        datasetType,
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Collections.singletonList(hasOwner),
        outgoingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        datasetType,
        Collections.singletonList(hasOwner),
        incomingRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Collections.singletonList(hasOwner),
        undirectedRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      },
      new Object[] {
        userType,
        Collections.singletonList(hasOwner),
        outgoingRelationships,
        Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
      },
      new Object[] {
        userType,
        Collections.singletonList(hasOwner),
        incomingRelationships,
        Collections.emptyList()
      },
      new Object[] {
        userType,
        Collections.singletonList(hasOwner),
        undirectedRelationships,
        Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
      }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesDestinationTypeTests")
  public void testFindRelatedEntitiesDestinationType(
      String entityTypeFilter,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    doTestFindRelatedEntities(
        anyType,
        entityTypeFilter != null ? ImmutableList.of(entityTypeFilter) : null,
        relationshipTypes,
        relationships,
        expectedRelatedEntities);
  }

  private void doTestFindRelatedEntities(
      final List<String> sourceType,
      final List<String> destinationType,
      final List<String> relationshipTypes,
      final RelationshipFilter relationshipFilter,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            sourceType,
            EMPTY_FILTER,
            destinationType,
            EMPTY_FILTER,
            relationshipTypes,
            relationshipFilter,
            0,
            10);

    assertEqualsAnyOrder(relatedEntities, expectedRelatedEntities);
  }

  private void doTestFindRelatedEntitiesEntityType(
      @Nullable List<String> sourceType,
      @Nullable List<String> destinationType,
      @Nonnull String relationshipType,
      @Nonnull RelationshipFilter relationshipFilter,
      @Nonnull GraphService service,
      @Nonnull RelatedEntity... expectedEntities) {
    RelatedEntitiesResult actualEntities =
        service.findRelatedEntities(
            operationContext,
            sourceType,
            EMPTY_FILTER,
            destinationType,
            EMPTY_FILTER,
            Collections.singletonList(relationshipType),
            relationshipFilter,
            0,
            100);
    assertEqualsAnyOrder(actualEntities, Arrays.asList(expectedEntities));
  }

  @Test
  public void testFindRelatedEntitiesNullSourceType() throws Exception {
    GraphService service = getGraphService();

    Urn nullUrn = createFromString("urn:li:null:(urn:li:null:Null)");
    assertNotNull(nullUrn);
    RelatedEntity nullRelatedEntity = new RelatedEntity(downstreamOf, nullUrn.toString());

    doTestFindRelatedEntitiesEntityType(
        anyType, ImmutableList.of("null"), downstreamOf, outgoingRelationships, service);
    doTestFindRelatedEntitiesEntityType(
        anyType, null, downstreamOf, outgoingRelationships, service);

    service.addEdge(new Edge(dataset2Urn, dataset1Urn, downstreamOf, null, null, null, null, null));
    syncAfterWrite();
    doTestFindRelatedEntitiesEntityType(
        anyType, ImmutableList.of("null"), downstreamOf, outgoingRelationships, service);
    doTestFindRelatedEntitiesEntityType(
        anyType,
        null,
        downstreamOf,
        outgoingRelationships,
        service,
        downstreamOfDatasetOneRelatedEntity);

    service.addEdge(new Edge(dataset1Urn, nullUrn, downstreamOf, null, null, null, null, null));
    syncAfterWrite();
    doTestFindRelatedEntitiesEntityType(
        anyType,
        ImmutableList.of("null"),
        downstreamOf,
        outgoingRelationships,
        service,
        nullRelatedEntity);
    doTestFindRelatedEntitiesEntityType(
        anyType,
        null,
        downstreamOf,
        outgoingRelationships,
        service,
        nullRelatedEntity,
        downstreamOfDatasetOneRelatedEntity);
  }

  @Test
  public void testFindRelatedEntitiesNullDestinationType() throws Exception {
    GraphService service = getGraphService();

    Urn nullUrn = createFromString("urn:li:null:(urn:li:null:Null)");
    assertNotNull(nullUrn);
    RelatedEntity nullRelatedEntity = new RelatedEntity(downstreamOf, nullUrn.toString());

    doTestFindRelatedEntitiesEntityType(
        anyType, ImmutableList.of("null"), downstreamOf, outgoingRelationships, service);
    doTestFindRelatedEntitiesEntityType(
        anyType, null, downstreamOf, outgoingRelationships, service);

    service.addEdge(new Edge(dataset2Urn, dataset1Urn, downstreamOf, null, null, null, null, null));
    syncAfterWrite();
    doTestFindRelatedEntitiesEntityType(
        anyType, ImmutableList.of("null"), downstreamOf, outgoingRelationships, service);
    doTestFindRelatedEntitiesEntityType(
        anyType,
        null,
        downstreamOf,
        outgoingRelationships,
        service,
        downstreamOfDatasetOneRelatedEntity);

    service.addEdge(new Edge(dataset1Urn, nullUrn, downstreamOf, null, null, null, null, null));
    syncAfterWrite();
    doTestFindRelatedEntitiesEntityType(
        anyType,
        ImmutableList.of("null"),
        downstreamOf,
        outgoingRelationships,
        service,
        nullRelatedEntity);
    doTestFindRelatedEntitiesEntityType(
        anyType,
        null,
        downstreamOf,
        outgoingRelationships,
        service,
        nullRelatedEntity,
        downstreamOfDatasetOneRelatedEntity);
  }

  @Test
  public void testFindRelatedEntitiesRelationshipTypes() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult allOutgoingRelatedEntities =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    // All DownstreamOf relationships, outgoing (destination)
    assertEqualsAnyOrder(
        allOutgoingRelatedEntities,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity,
            knowsUserOneRelatedEntity, knowsUserTwoRelatedEntity,
            downstreamOfSchemaFieldTwoVia, downstreamOfSchemaFieldTwo));

    RelatedEntitiesResult allIncomingRelatedEntities =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            incomingRelationships,
            0,
            100);
    // All DownstreamOf relationships, incoming (source)
    assertEqualsAnyOrder(
        allIncomingRelatedEntities,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity,
            hasOwnerDatasetFourRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            downstreamOfSchemaFieldOneVia,
            downstreamOfSchemaFieldOne));

    RelatedEntitiesResult allUnknownRelationshipTypeRelatedEntities =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList("unknownRelationshipType", "unseenRelationshipType"),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(allUnknownRelationshipTypeRelatedEntities, Collections.emptyList());

    RelatedEntitiesResult someUnknownRelationshipTypeRelatedEntities =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList("unknownRelationshipType", downstreamOf),
            outgoingRelationships,
            0,
            100);
    // All DownstreamOf relationships, outgoing (destination)
    assertEqualsAnyOrder(
        someUnknownRelationshipTypeRelatedEntities,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfSchemaFieldTwoVia,
            downstreamOfSchemaFieldTwo));
  }

  @Test
  public void testFindRelatedEntitiesNoRelationshipTypes() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Collections.emptyList(),
            outgoingRelationships,
            0,
            10);

    assertEquals(relatedEntities.entities, Collections.emptyList());

    // does the test actually test something? is the Collections.emptyList() the only reason why we
    // did not get any related urns?
    RelatedEntitiesResult relatedEntitiesAll =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            10);

    assertNotEquals(relatedEntitiesAll.entities, Collections.emptyList());
  }

  @Test
  public void testFindRelatedEntitiesAllFilters() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(datasetType),
            newFilter("urn", dataset1UrnString),
            ImmutableList.of(userType),
            newFilter("urn", userOneUrnString),
            Collections.singletonList(hasOwner),
            outgoingRelationships,
            0,
            10);

    assertEquals(relatedEntities.entities, Collections.singletonList(hasOwnerUserOneRelatedEntity));

    relatedEntities =
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(datasetType),
            newFilter("urn", dataset1UrnString),
            ImmutableList.of(userType),
            newFilter("urn", userTwoUrnString),
            Collections.singletonList(hasOwner),
            incomingRelationships,
            0,
            10);

    assertEquals(relatedEntities.entities, Collections.emptyList());
  }

  @Test
  public void testFindRelatedEntitiesMultipleEntityTypes() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(datasetType, userType),
            newFilter("urn", dataset1UrnString),
            ImmutableList.of(datasetType, userType),
            newFilter("urn", userOneUrnString),
            Collections.singletonList(hasOwner),
            outgoingRelationships,
            0,
            10);

    assertEquals(relatedEntities.entities, Collections.singletonList(hasOwnerUserOneRelatedEntity));

    relatedEntities =
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(datasetType, userType),
            newFilter("urn", dataset1UrnString),
            ImmutableList.of(datasetType, userType),
            newFilter("urn", userTwoUrnString),
            Collections.singletonList(hasOwner),
            incomingRelationships,
            0,
            10);

    assertEquals(relatedEntities.entities, Collections.emptyList());
  }

  @Test
  public void testFindRelatedEntitiesOffsetAndCount() throws Exception {
    GraphService service = getPopulatedGraphService();

    // populated graph asserted in testPopulatedGraphService
    RelatedEntitiesResult allRelatedEntities =
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(datasetType),
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);

    List<RelatedEntity> individualRelatedEntities = new ArrayList<>();
    IntStream.range(0, allRelatedEntities.entities.size())
        .forEach(
            idx ->
                individualRelatedEntities.addAll(
                    service.findRelatedEntities(
                            operationContext,
                            ImmutableList.of(datasetType),
                            EMPTY_FILTER,
                            anyType,
                            EMPTY_FILTER,
                            Arrays.asList(downstreamOf, hasOwner, knowsUser),
                            outgoingRelationships,
                            idx,
                            1)
                        .entities));
    Assert.assertEquals(individualRelatedEntities, allRelatedEntities.entities);
  }

  @DataProvider(name = "RemoveEdgesFromNodeTests")
  public Object[][] getRemoveEdgesFromNodeTests() {
    return new Object[][] {
      new Object[] {
        dataset2Urn,
        Collections.singletonList(downstreamOf),
        outgoingRelationships,
        Collections.singletonList(downstreamOfDatasetOneRelatedEntity),
        Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity),
        Collections.emptyList(),
        Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        dataset2Urn,
        Collections.singletonList(downstreamOf),
        incomingRelationships,
        Collections.singletonList(downstreamOfDatasetOneRelatedEntity),
        Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity),
        Collections.singletonList(downstreamOfDatasetOneRelatedEntity),
        Collections.emptyList(),
      },
      new Object[] {
        dataset2Urn,
        Collections.singletonList(downstreamOf),
        undirectedRelationships,
        Collections.singletonList(downstreamOfDatasetOneRelatedEntity),
        Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity),
        Collections.emptyList(),
        Collections.emptyList()
      },
      new Object[] {
        userOneUrn,
        Arrays.asList(hasOwner, knowsUser),
        outgoingRelationships,
        Collections.singletonList(knowsUserTwoRelatedEntity),
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            knowsUserTwoRelatedEntity),
        Collections.emptyList(),
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            knowsUserTwoRelatedEntity)
      },
      new Object[] {
        userOneUrn,
        Arrays.asList(hasOwner, knowsUser),
        incomingRelationships,
        Collections.singletonList(knowsUserTwoRelatedEntity),
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            knowsUserTwoRelatedEntity),
        Collections.singletonList(knowsUserTwoRelatedEntity),
        Collections.emptyList()
      },
      new Object[] {
        userOneUrn,
        Arrays.asList(hasOwner, knowsUser),
        undirectedRelationships,
        Collections.singletonList(knowsUserTwoRelatedEntity),
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            knowsUserTwoRelatedEntity),
        Collections.emptyList(),
        Collections.emptyList()
      }
    };
  }

  @Test(dataProvider = "RemoveEdgesFromNodeTests")
  public void testRemoveEdgesFromNode(
      @Nonnull Urn nodeToRemoveFrom,
      @Nonnull List<String> relationTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      List<RelatedEntity> expectedOutgoingRelatedUrnsBeforeRemove,
      List<RelatedEntity> expectedIncomingRelatedUrnsBeforeRemove,
      List<RelatedEntity> expectedOutgoingRelatedUrnsAfterRemove,
      List<RelatedEntity> expectedIncomingRelatedUrnsAfterRemove)
      throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> allOtherRelationTypes =
        allRelationshipTypes.stream()
            .filter(relation -> !relationTypes.contains(relation))
            .collect(Collectors.toList());
    assertFalse(allOtherRelationTypes.isEmpty());

    RelatedEntitiesResult actualOutgoingRelatedUrnsBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            relationTypes,
            outgoingRelationships,
            0,
            100);
    RelatedEntitiesResult actualIncomingRelatedUrnsBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            relationTypes,
            incomingRelationships,
            0,
            100);
    assertEqualsAnyOrder(
        actualOutgoingRelatedUrnsBeforeRemove, expectedOutgoingRelatedUrnsBeforeRemove);
    assertEqualsAnyOrder(
        actualIncomingRelatedUrnsBeforeRemove, expectedIncomingRelatedUrnsBeforeRemove);

    // we expect these do not change
    RelatedEntitiesResult relatedEntitiesOfOtherOutgoingRelationTypesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            allOtherRelationTypes,
            outgoingRelationships,
            0,
            100);
    RelatedEntitiesResult relatedEntitiesOfOtherIncomingRelationTypesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            allOtherRelationTypes,
            incomingRelationships,
            0,
            100);

    service.removeEdgesFromNode(
        operationContext, nodeToRemoveFrom, relationTypes, relationshipFilter);
    syncAfterWrite();

    RelatedEntitiesResult actualOutgoingRelatedUrnsAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            relationTypes,
            outgoingRelationships,
            0,
            100);
    RelatedEntitiesResult actualIncomingRelatedUrnsAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            relationTypes,
            incomingRelationships,
            0,
            100);
    assertEqualsAnyOrder(
        actualOutgoingRelatedUrnsAfterRemove, expectedOutgoingRelatedUrnsAfterRemove);
    assertEqualsAnyOrder(
        actualIncomingRelatedUrnsAfterRemove, expectedIncomingRelatedUrnsAfterRemove);

    // assert these did not change
    RelatedEntitiesResult relatedEntitiesOfOtherOutgoingRelationTypesAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            allOtherRelationTypes,
            outgoingRelationships,
            0,
            100);
    RelatedEntitiesResult relatedEntitiesOfOtherIncomingRelationTypesAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            allOtherRelationTypes,
            incomingRelationships,
            0,
            100);
    assertEqualsAnyOrder(
        relatedEntitiesOfOtherOutgoingRelationTypesAfterRemove,
        relatedEntitiesOfOtherOutgoingRelationTypesBeforeRemove);
    assertEqualsAnyOrder(
        relatedEntitiesOfOtherIncomingRelationTypesAfterRemove,
        relatedEntitiesOfOtherIncomingRelationTypesBeforeRemove);
  }

  @Test
  public void testRemoveEdgesFromNodeNoRelationshipTypes() throws Exception {
    GraphService service = getPopulatedGraphService();
    Urn nodeToRemoveFrom = dataset1Urn;

    // populated graph asserted in testPopulatedGraphService
    RelatedEntitiesResult relatedOutgoingEntitiesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);

    // can be replaced with a single removeEdgesFromNode and undirectedRelationships once supported
    // by all implementations
    service.removeEdgesFromNode(
        operationContext, nodeToRemoveFrom, Collections.emptyList(), outgoingRelationships);
    service.removeEdgesFromNode(
        operationContext, nodeToRemoveFrom, Collections.emptyList(), incomingRelationships);
    syncAfterWrite();

    RelatedEntitiesResult relatedOutgoingEntitiesAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(relatedOutgoingEntitiesAfterRemove, relatedOutgoingEntitiesBeforeRemove);

    // does the test actually test something? is the Collections.emptyList() the only reason why we
    // did not see changes?
    service.removeEdgesFromNode(
        operationContext,
        nodeToRemoveFrom,
        Arrays.asList(downstreamOf, hasOwner, knowsUser),
        outgoingRelationships);
    service.removeEdgesFromNode(
        operationContext,
        nodeToRemoveFrom,
        Arrays.asList(downstreamOf, hasOwner, knowsUser),
        incomingRelationships);
    syncAfterWrite();

    RelatedEntitiesResult relatedOutgoingEntitiesAfterRemoveAll =
        service.findRelatedEntities(
            operationContext,
            anyType,
            newFilter("urn", nodeToRemoveFrom.toString()),
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(relatedOutgoingEntitiesAfterRemoveAll, Collections.emptyList());
  }

  @Test
  public void testRemoveEdgesFromUnknownNode() throws Exception {
    GraphService service = getPopulatedGraphService();
    Urn nodeToRemoveFrom = unknownUrn;

    // populated graph asserted in testPopulatedGraphService
    RelatedEntitiesResult relatedOutgoingEntitiesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);

    // can be replaced with a single removeEdgesFromNode and undirectedRelationships once supported
    // by all implementations
    service.removeEdgesFromNode(
        operationContext,
        nodeToRemoveFrom,
        Arrays.asList(downstreamOf, hasOwner, knowsUser),
        outgoingRelationships);
    service.removeEdgesFromNode(
        operationContext,
        nodeToRemoveFrom,
        Arrays.asList(downstreamOf, hasOwner, knowsUser),
        incomingRelationships);
    syncAfterWrite();

    RelatedEntitiesResult relatedOutgoingEntitiesAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(relatedOutgoingEntitiesAfterRemove, relatedOutgoingEntitiesBeforeRemove);
  }

  @Test
  public void testRemoveNode() throws Exception {
    GraphService service = getPopulatedGraphService();

    service.removeNode(operationContext, dataset2Urn);
    syncAfterWrite();

    // assert the modified graph
    // All downstreamOf, hasOwner, knowsUser relationships minus datasetTwo's, outgoing
    assertEqualsAnyOrder(
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100),
        Arrays.asList(
            hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity,
            knowsUserOneRelatedEntity, knowsUserTwoRelatedEntity,
            downstreamOfSchemaFieldTwoVia, downstreamOfSchemaFieldTwo));
  }

  @Test
  public void testRemoveUnknownNode() throws Exception {
    GraphService service = getPopulatedGraphService();

    // populated graph asserted in testPopulatedGraphService
    RelatedEntitiesResult entitiesBeforeRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);

    service.removeNode(operationContext, unknownUrn);
    syncAfterWrite();

    RelatedEntitiesResult entitiesAfterRemove =
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(entitiesBeforeRemove, entitiesAfterRemove);
  }

  @Test
  public void testClear() throws Exception {
    GraphService service = getPopulatedGraphService();

    // populated graph asserted in testPopulatedGraphService

    service.clear();
    syncAfterWrite();

    // assert the modified graph: check all nodes related to upstreamOf and nextVersionOf edges
    // again
    assertEqualsAnyOrder(
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(datasetType),
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Collections.singletonList(downstreamOf),
            outgoingRelationships,
            0,
            100),
        Collections.emptyList());
    assertEqualsAnyOrder(
        service.findRelatedEntities(
            operationContext,
            ImmutableList.of(userType),
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Collections.singletonList(hasOwner),
            outgoingRelationships,
            0,
            100),
        Collections.emptyList());
    assertEqualsAnyOrder(
        service.findRelatedEntities(
            operationContext,
            anyType,
            EMPTY_FILTER,
            ImmutableList.of(userType),
            EMPTY_FILTER,
            Collections.singletonList(knowsUser),
            outgoingRelationships,
            0,
            100),
        Collections.emptyList());
  }

  private List<Edge> getFullyConnectedGraph(
      int nodes, List<String> relationshipTypes, @Nullable List<String> entityTypes) {
    List<Edge> edges = new ArrayList<>();

    for (int sourceNode = 1; sourceNode <= nodes; sourceNode++) {
      for (int destinationNode = 1; destinationNode <= nodes; destinationNode++) {
        for (String relationship : relationshipTypes) {
          String typeString;
          if (entityTypes != null) {
            typeString = entityTypes.get(sourceNode % entityTypes.size());
          } else {
            typeString = "type" + sourceNode % 3;
          }
          Urn source =
              createFromString("urn:li:" + typeString + ":(urn:li:node" + sourceNode + ")");
          if (entityTypes == null) {
            typeString = "type" + destinationNode % 3;
          }
          Urn destination =
              createFromString("urn:li:" + typeString + ":(urn:li:node" + destinationNode + ")");

          edges.add(new Edge(source, destination, relationship, null, null, null, null, null));
        }
      }
    }

    return edges;
  }

  @Test
  public void testConcurrentAddEdge() throws Exception {
    final GraphService service = getGraphService();

    // TODO: Refactor to use executor pool rather than raw runnables to avoid thread overload
    // too many edges may cause too many threads throwing
    // java.util.concurrent.RejectedExecutionException: Thread limit exceeded replacing blocked
    // worker
    int nodes = 5;
    int relationshipTypes = 3;
    List<String> allRelationships =
        IntStream.range(1, relationshipTypes + 1)
            .mapToObj(id -> "relationship" + id)
            .collect(Collectors.toList());
    List<Edge> edges = getFullyConnectedGraph(nodes, allRelationships, null);

    Stream<Runnable> operations = edges.stream().map(edge -> () -> service.addEdge(edge));

    doTestConcurrentOp(operations);
    syncAfterWrite();

    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            null,
            EMPTY_FILTER,
            null,
            EMPTY_FILTER,
            allRelationships,
            outgoingRelationships,
            0,
            edges.size());

    Set<RelatedEntity> expectedRelatedEntities = convertEdgesToRelatedEntities(edges);
    assertEquals(
        deduplicateRelatedEntitiesByRelationshipTypeAndDestination(relatedEntities),
        expectedRelatedEntities);
  }

  protected Set<RelatedEntity> convertEdgesToRelatedEntities(List<Edge> edges) {
    return edges.stream()
        .map(
            edge -> new RelatedEntity(edge.getRelationshipType(), edge.getDestination().toString()))
        .collect(Collectors.toSet());
  }

  protected Set<RelatedEntity> deduplicateRelatedEntitiesByRelationshipTypeAndDestination(
      RelatedEntitiesResult relatedEntitiesResult) {
    return Set.copyOf(relatedEntitiesResult.getEntities());
  }

  @Test
  public void testConcurrentRemoveEdgesFromNode() throws Exception {
    final GraphService service = getGraphService();

    int nodes = 5;
    int relationshipTypes = 3;
    List<String> allRelationships =
        IntStream.range(1, relationshipTypes + 1)
            .mapToObj(id -> "relationship" + id)
            .collect(Collectors.toList());
    List<Edge> edges = getFullyConnectedGraph(nodes, allRelationships, null);

    // add fully connected graph
    edges.forEach(service::addEdge);
    syncAfterWrite();

    // assert the graph is there
    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            null,
            EMPTY_FILTER,
            null,
            EMPTY_FILTER,
            allRelationships,
            outgoingRelationships,
            0,
            edges.size());
    assertEquals(
        deduplicateRelatedEntitiesByRelationshipTypeAndDestination(relatedEntities).size(),
        nodes * relationshipTypes);

    // delete all edges concurrently
    Stream<Runnable> operations =
        edges.stream()
            .map(
                edge ->
                    () ->
                        service.removeEdgesFromNode(
                            operationContext,
                            edge.getSource(),
                            Collections.singletonList(edge.getRelationshipType()),
                            outgoingRelationships));
    doTestConcurrentOp(operations);
    syncAfterWrite();

    // assert the graph is gone
    RelatedEntitiesResult relatedEntitiesAfterDeletion =
        service.findRelatedEntities(
            operationContext,
            null,
            EMPTY_FILTER,
            null,
            EMPTY_FILTER,
            allRelationships,
            outgoingRelationships,
            0,
            nodes * relationshipTypes * 2);
    assertEquals(relatedEntitiesAfterDeletion.entities.size(), 0);
  }

  @Test
  public void testConcurrentRemoveNodes() throws Exception {
    final GraphService service = getGraphService();

    // too many edges may cause too many threads throwing
    // java.util.concurrent.RejectedExecutionException: Thread limit exceeded replacing blocked
    // worker
    int nodes = 5;
    int relationshipTypes = 3;
    List<String> allRelationships =
        IntStream.range(1, relationshipTypes + 1)
            .mapToObj(id -> "relationship" + id)
            .collect(Collectors.toList());
    List<Edge> edges = getFullyConnectedGraph(nodes, allRelationships, null);

    // add fully connected graph
    edges.forEach(service::addEdge);
    syncAfterWrite();

    // assert the graph is there
    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            null,
            EMPTY_FILTER,
            null,
            EMPTY_FILTER,
            allRelationships,
            outgoingRelationships,
            0,
            edges.size());
    assertEquals(
        deduplicateRelatedEntitiesByRelationshipTypeAndDestination(relatedEntities).size(),
        nodes * relationshipTypes);

    // remove all nodes concurrently
    // nodes will be removed multiple times
    Stream<Runnable> operations =
        edges.stream().map(edge -> () -> service.removeNode(operationContext, edge.getSource()));
    doTestConcurrentOp(operations);
    syncAfterWrite();

    // assert the graph is gone
    RelatedEntitiesResult relatedEntitiesAfterDeletion =
        service.findRelatedEntities(
            operationContext,
            null,
            EMPTY_FILTER,
            null,
            EMPTY_FILTER,
            allRelationships,
            outgoingRelationships,
            0,
            nodes * relationshipTypes * 2);
    assertEquals(relatedEntitiesAfterDeletion.entities.size(), 0);
  }

  private void doTestConcurrentOp(Stream<Runnable> operations) throws Exception {
    final Queue<Throwable> throwables = new ConcurrentLinkedQueue<>();
    ExecutorService executorPool =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);

    List<Callable<Void>> callables =
        operations
            .map(
                operation ->
                    (Callable<Void>)
                        () -> {
                          try {
                            operation.run();
                          } catch (Throwable t) {
                            throwables.add(t);
                            t.printStackTrace();
                          }
                          return null;
                        })
            .collect(Collectors.toList());
    try {
      List<Future<Void>> futures =
          executorPool.invokeAll(
              callables, getTestConcurrentOpTimeout().toMillis(), TimeUnit.MILLISECONDS);
      futures.forEach(
          future -> {
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              System.err.println(
                  System.currentTimeMillis()
                      + ": unable to complete execution of concurrent operations in time");
            }
          });
    } catch (InterruptedException e) {
      System.err.println(
          System.currentTimeMillis()
              + ": unable to complete execution of concurrent operations in time");
      throw e;
    }
    throwables.forEach(
        throwable ->
            System.err.printf(
                System.currentTimeMillis() + ": exception occurred: %s%n", throwable));
    assertTrue(throwables.isEmpty());
  }

  @DataProvider(name = "trueFalse")
  public static Object[] trueFalse() {
    return new Object[] {true, false};
  }

  @Test(dataProvider = "trueFalse")
  public void testPopulatedGraphServiceGetLineageMultihop(Boolean attemptMultiPathAlgo)
      throws Exception {

    GraphService service = getLineagePopulatedGraphService(attemptMultiPathAlgo);
    // Implementations other than Neo4J and DGraph explore more of the graph to discover nodes at
    // multiple hops
    boolean expandedGraphAlgoEnabled =
        (!((service instanceof Neo4jGraphService) || (service instanceof DgraphGraphService)));

    EntityLineageResult upstreamLineage =
        service.getLineage(operationContext, dataset1Urn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineage.getTotal().intValue(), 0);
    assertEquals(upstreamLineage.getRelationships().size(), 0);

    EntityLineageResult downstreamLineage =
        service.getLineage(operationContext, dataset1Urn, LineageDirection.DOWNSTREAM, 0, 1000, 2);

    assertEquals(downstreamLineage.getTotal().intValue(), 5);
    assertEquals(downstreamLineage.getRelationships().size(), 5);
    Map<Urn, LineageRelationship> relationships =
        downstreamLineage.getRelationships().stream()
            .collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    Set<Urn> entities = relationships.keySet().stream().collect(Collectors.toUnmodifiableSet());
    assertEquals(entities.size(), 5);
    assertTrue(relationships.containsKey(dataset2Urn));
    assertEquals(relationships.get(dataJobTwoUrn).getDegree(), 1);
    assertTrue(relationships.containsKey(dataset3Urn));
    assertEquals(relationships.get(dataset3Urn).getDegree(), 2);
    assertTrue(relationships.containsKey(dataset4Urn));
    assertEquals(relationships.get(dataset4Urn).getDegree(), 2);
    assertTrue(relationships.containsKey(dataJobOneUrn));
    assertEquals(relationships.get(dataJobOneUrn).getDegree(), 1);
    // dataJobOne is present both at degree 1 and degree 2
    if (expandedGraphAlgoEnabled && attemptMultiPathAlgo) {
      relationships.get(dataJobOneUrn).getDegrees().contains(Integer.valueOf(1));
      relationships.get(dataJobOneUrn).getDegrees().contains(Integer.valueOf(2));
    }
    assertTrue(relationships.containsKey(dataJobTwoUrn));
    assertEquals(relationships.get(dataJobTwoUrn).getDegree(), 1);

    upstreamLineage =
        service.getLineage(operationContext, dataset3Urn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineage.getTotal().intValue(), 3);
    assertEquals(upstreamLineage.getRelationships().size(), 3);
    relationships =
        upstreamLineage.getRelationships().stream()
            .collect(Collectors.toMap(LineageRelationship::getEntity, Function.identity()));
    assertTrue(relationships.containsKey(dataset1Urn));
    assertEquals(relationships.get(dataset1Urn).getDegree(), 2);
    assertTrue(relationships.containsKey(dataset2Urn));
    assertEquals(relationships.get(dataset2Urn).getDegree(), 1);
    assertTrue(relationships.containsKey(dataJobOneUrn));
    assertEquals(relationships.get(dataJobOneUrn).getDegree(), 1);

    downstreamLineage =
        service.getLineage(operationContext, dataset3Urn, LineageDirection.DOWNSTREAM, 0, 1000, 2);
    assertEquals(downstreamLineage.getTotal().intValue(), 0);
    assertEquals(downstreamLineage.getRelationships().size(), 0);
  }

  @Test
  public void testHighlyConnectedGraphWalk() throws Exception {
    final GraphService service = getGraphService();
    List<String> allRelationships = Collections.singletonList(downstreamOf);
    List<Edge> edges = createHighlyConnectedGraph();

    Stream<Runnable> operations = edges.stream().map(edge -> () -> service.addEdge(edge));

    doTestConcurrentOp(operations);
    syncAfterWrite();

    Set<RelatedEntity> expectedRelatedEntities = convertEdgesToRelatedEntities(edges);
    RelatedEntitiesResult relatedEntities =
        service.findRelatedEntities(
            operationContext,
            null,
            EMPTY_FILTER,
            null,
            EMPTY_FILTER,
            allRelationships,
            outgoingRelationships,
            0,
            edges.size());
    assertEquals(
        deduplicateRelatedEntitiesByRelationshipTypeAndDestination(relatedEntities),
        expectedRelatedEntities);

    Urn root = dataset1Urn;
    OperationContext limitedHopOpContext =
        operationContext.withLineageFlags(f -> f.setEntitiesExploredPerHopLimit(5));

    EntityLineageResult lineageResult =
        getGraphService(false)
            .getLineage(
                limitedHopOpContext,
                root,
                LineageDirection.UPSTREAM,
                new GraphFilters(
                    relatedEntities.getEntities().stream()
                        .map(RelatedEntity::getUrn)
                        .map(UrnUtils::getUrn)
                        .map(Urn::getEntityType)
                        .distinct()
                        .collect(Collectors.toList())),
                0,
                1000,
                100);
    // Unable to explore all paths because multi is disabled, but will be at least 5 since it will
    // explore 5 edges
    assertTrue(
        lineageResult.getRelationships().size() >= 5
            && lineageResult.getRelationships().size() < 20,
        "Size was: " + lineageResult.getRelationships().size());
    LineageRelationshipArray relationships = lineageResult.getRelationships();
    int maxDegree =
        relationships.stream()
            .flatMap(relationship -> relationship.getDegrees().stream())
            .reduce(0, Math::max);
    assertTrue(maxDegree >= 1);

    EntityLineageResult lineageResultMulti =
        getGraphService(true)
            .getLineage(
                limitedHopOpContext,
                root,
                LineageDirection.UPSTREAM,
                new GraphFilters(
                    relatedEntities.getEntities().stream()
                        .map(RelatedEntity::getUrn)
                        .map(UrnUtils::getUrn)
                        .map(Urn::getEntityType)
                        .distinct()
                        .collect(Collectors.toList())),
                0,
                1000,
                100);

    assertTrue(
        lineageResultMulti.getRelationships().size() >= 5
            && lineageResultMulti.getRelationships().size() <= 20,
        "Size was: " + lineageResultMulti.getRelationships().size());
    relationships = lineageResultMulti.getRelationships();
    maxDegree =
        relationships.stream()
            .flatMap(relationship -> relationship.getDegrees().stream())
            .reduce(0, Math::max);
    assertTrue(maxDegree >= 2);

    // Reset graph service
    getGraphService();
  }

  protected List<Edge> createHighlyConnectedGraph() {
    List<Edge> graph = new ArrayList<>();
    for (Urn sourceUrn : datasetUrns) {
      for (Urn destUrn : datasetUrns) {
        if (sourceUrn.equals(destUrn)) {
          continue;
        }
        Edge edge =
            new Edge(
                sourceUrn, destUrn, downstreamOf, 0L, userOneUrn, 0L, userOneUrn, null, null, null);
        graph.add(edge);
      }
    }
    return graph;
  }
}
