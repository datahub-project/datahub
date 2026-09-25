package com.linkedin.datahub.graphql.resolvers.health;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetHealthResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityHealthBatchLoader;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.GenericTable;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

// TODO: Update this test once assertions summary has been added.
public class EntityHealthResolverTest {

  private static final String TEST_DATASET_URN = "urn:li:dataset:(test,test,test)";
  private static final String TEST_ASSERTION_URN = "urn:li:assertion:test-guid";
  private static final String TEST_ASSERTION_URN_2 = "urn:li:assertion:test-guid-2";
  private static final String TEST_TEST_URN = "urn:li:test:test-guid";

  @Test
  public void testGetSuccessHealthy() throws Exception {
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(500),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship()
                                .setEntity(Urn.createFromString(TEST_ASSERTION_URN))
                                .setType("Asserts")))));

    Mockito.when(
            mockAspectService.getAggregatedStats(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new GenericTable()
                .setColumnNames(
                    new StringArray(ImmutableList.of("assertionUrn", "type", "timestampMillis")))
                .setColumnTypes(new StringArray("string", "string", "long"))
                .setRows(
                    new StringArrayArray(
                        ImmutableList.of(
                            new StringArray(
                                ImmutableList.of(TEST_ASSERTION_URN, "SUCCESS", "0"))))));

    DatasetHealthResolver resolver = new DatasetHealthResolver(graphClient, mockAspectService);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.PASS);
  }

  @Test
  public void testGetSuccessNullHealth() throws Exception {
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    // 0 associated assertions, meaning we don't report any health.
    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(500),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(0)
                .setTotal(0)
                .setRelationships(new EntityRelationshipArray(Collections.emptyList())));

    DatasetHealthResolver resolver = new DatasetHealthResolver(graphClient, mockAspectService);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);

    Mockito.verify(mockAspectService, Mockito.times(0))
        .getAggregatedStats(
            any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testGetSuccessUnhealthy() throws Exception {
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(500),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(0)
                .setTotal(2)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship()
                                .setEntity(Urn.createFromString(TEST_ASSERTION_URN))
                                .setType("Asserts"),
                            new EntityRelationship()
                                .setEntity(Urn.createFromString(TEST_ASSERTION_URN_2))
                                .setType("Asserts")))));

    Mockito.when(
            mockAspectService.getAggregatedStats(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new GenericTable()
                .setColumnNames(
                    new StringArray(ImmutableList.of("assertionUrn", "type", "timestampMillis")))
                .setColumnTypes(new StringArray("string", "string", "long"))
                .setRows(
                    new StringArrayArray(
                        ImmutableList.of(
                            new StringArray(ImmutableList.of(TEST_ASSERTION_URN, "SUCCESS", "0")),
                            new StringArray(
                                ImmutableList.of(TEST_ASSERTION_URN_2, "FAILURE", "0"))))));

    DatasetHealthResolver resolver = new DatasetHealthResolver(graphClient, mockAspectService);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
  }

  @Test
  public void testComputeTestsHealthFailingTests() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    TestResults testResults = new TestResults();
    TestResult failingResult = new TestResult();
    failingResult.setTest(Urn.createFromString(TEST_TEST_URN));
    failingResult.setType(TestResultType.FAILURE);
    testResults.setFailing(new TestResultArray(ImmutableList.of(failingResult)));
    testResults.setPassing(new TestResultArray(Collections.emptyList()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.TEST_RESULTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(testResults.data())));
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);

    Mockito.when(
            mockEntityClient.getV2(
                Mockito.any(),
                Mockito.eq("dataset"),
                Mockito.eq(Urn.createFromString(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME))))
        .thenReturn(entityResponse);

    // Disable assertions and incidents, enable tests only
    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getType(), HealthStatusType.TESTS);
    assertEquals(result.get(0).getMessage(), "1 of 1 tests failing");
  }

  @Test
  public void testComputeTestsHealthAllPassing() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    TestResults testResults = new TestResults();
    TestResult passingResult = new TestResult();
    passingResult.setTest(Urn.createFromString(TEST_TEST_URN));
    passingResult.setType(TestResultType.SUCCESS);
    testResults.setPassing(new TestResultArray(ImmutableList.of(passingResult)));
    testResults.setFailing(new TestResultArray(Collections.emptyList()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.TEST_RESULTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(testResults.data())));
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);

    Mockito.when(
            mockEntityClient.getV2(
                Mockito.any(),
                Mockito.eq("dataset"),
                Mockito.eq(Urn.createFromString(TEST_DATASET_URN)),
                Mockito.eq(ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME))))
        .thenReturn(entityResponse);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.PASS);
    assertEquals(result.get(0).getType(), HealthStatusType.TESTS);
    assertEquals(result.get(0).getMessage(), "All tests are passing");
  }

  @Test
  public void testComputeTestsHealthNullEntityResponse() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any())).thenReturn(null);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testComputeTestsHealthNoTestResultsAspect() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(new EnvelopedAspectMap());

    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testComputeTestsHealthZeroTests() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    TestResults testResults = new TestResults();
    testResults.setPassing(new TestResultArray(Collections.emptyList()));
    testResults.setFailing(new TestResultArray(Collections.emptyList()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.TEST_RESULTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(testResults.data())));
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);

    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testComputeTestsHealthDisabled() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, false));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
    Mockito.verify(mockEntityClient, Mockito.times(0)).getV2(any(), any(), any(), any());
  }

  @Test
  public void testComputeTestsHealthMultipleFailingAndPassing() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    TestResults testResults = new TestResults();
    TestResult passing1 = new TestResult();
    passing1.setTest(Urn.createFromString("urn:li:test:pass-1"));
    passing1.setType(TestResultType.SUCCESS);
    TestResult failing1 = new TestResult();
    failing1.setTest(Urn.createFromString("urn:li:test:fail-1"));
    failing1.setType(TestResultType.FAILURE);
    TestResult failing2 = new TestResult();
    failing2.setTest(Urn.createFromString("urn:li:test:fail-2"));
    failing2.setType(TestResultType.FAILURE);
    testResults.setPassing(new TestResultArray(ImmutableList.of(passing1)));
    testResults.setFailing(new TestResultArray(ImmutableList.of(failing1, failing2)));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.TEST_RESULTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(testResults.data())));
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);

    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getType(), HealthStatusType.TESTS);
    assertEquals(result.get(0).getMessage(), "2 of 3 tests failing");
  }

  @Test
  public void testAllHealthTypesEnabledViaDefaultConstructor() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    EntityHealthResolver resolver =
        new EntityHealthResolver(mockEntityClient, mockGraphClient, mockAspectService);

    Mockito.when(
            mockEntityClient.filter(any(), any(), any(), any(), Mockito.anyInt(), Mockito.anyInt()))
        .thenReturn(new SearchResult().setNumEntities(0).setEntities(new SearchEntityArray()));
    Mockito.when(
            mockGraphClient.getRelatedEntities(
                any(), any(), any(), Mockito.anyInt(), Mockito.anyInt(), any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(0)
                .setTotal(0)
                .setRelationships(new EntityRelationshipArray()));
    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any())).thenReturn(null);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getType(), HealthStatusType.INCIDENTS);
    assertEquals(result.get(0).getStatus(), HealthStatus.PASS);
  }

  @Test
  public void testComputeTestsHealthRemoteInvocationException() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any()))
        .thenThrow(new RemoteInvocationException("test error"));

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, false, true));

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    List<Health> result = resolver.get(mockEnv).get();
    assertEquals(result.size(), 0);
  }

  @Test
  public void testBatchLoadFlagEnabledDelegatesToDataLoader() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService tsService = Mockito.mock(TimeseriesAspectService.class);

    FeatureFlags flags = new FeatureFlags();
    flags.setEntityHealthBatchLoadEnabled(true);

    EntityHealthResolver resolver =
        new EntityHealthResolver(
            entityClient,
            graphClient,
            tsService,
            new EntityHealthResolver.Config(true, true, true),
            flags);

    final Health expected = new Health();
    expected.setType(HealthStatusType.INCIDENTS);
    expected.setStatus(HealthStatus.FAIL);
    final List<Health> expectedList = ImmutableList.of(expected);

    @SuppressWarnings("unchecked")
    final DataLoader<EntityHealthBatchLoader.HealthQueryKey, List<Health>> loader =
        Mockito.mock(DataLoader.class);
    Mockito.when(loader.load(any())).thenReturn(CompletableFuture.completedFuture(expectedList));
    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.when(registry.getDataLoader(EntityHealthBatchLoader.LOADER_NAME))
        .thenReturn((DataLoader) loader);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getDataLoaderRegistry()).thenReturn(registry);
    Dataset parentDataset = new Dataset();
    parentDataset.setUrn(TEST_DATASET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDataset);

    final List<Health> result = resolver.get(mockEnv).get();

    // The flag path must delegate to the batch loader and return its result verbatim...
    assertEquals(result, expectedList);
    Mockito.verifyNoInteractions(graphClient, tsService);
    // ...keyed by the source urn with the resolver's configured dimensions.
    final ArgumentCaptor<EntityHealthBatchLoader.HealthQueryKey> keyCaptor =
        ArgumentCaptor.forClass(EntityHealthBatchLoader.HealthQueryKey.class);
    Mockito.verify(loader).load(keyCaptor.capture());
    final EntityHealthBatchLoader.HealthQueryKey key = keyCaptor.getValue();
    assertEquals(key.getUrn(), UrnUtils.getUrn(TEST_DATASET_URN));
    assertTrue(key.isAssertionsEnabled());
    assertTrue(key.isIncidentsEnabled());
    assertTrue(key.isTestsEnabled());
  }

  /**
   * Legacy per-entity path (flag off): a dataset with an active incident resolves to a failing
   * INCIDENTS health that carries the latest incident's urn and title. Exercises
   * computeIncidentsHealthForAsset (count > 0) and the getIncidentInfo aspect fetch.
   */
  @Test
  public void testLegacyActiveIncidentReportsFailingHealth() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    final Urn incidentUrn = Urn.createFromString("urn:li:incident:i1");
    Mockito.when(
            mockEntityClient.filter(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                any(),
                any(),
                Mockito.eq(0),
                Mockito.eq(1)))
        .thenReturn(
            new SearchResult()
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(new SearchEntity().setEntity(incidentUrn)))));

    final IncidentInfo info =
        new IncidentInfo()
            .setTitle("nightly load failed")
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(
                        new AuditStamp()
                            .setTime(999L)
                            .setActor(Urn.createFromString("urn:li:corpuser:t"))));
    final EnvelopedAspectMap incidentAspects = new EnvelopedAspectMap();
    incidentAspects.put(
        Constants.INCIDENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(info.data())));
    Mockito.when(
            mockEntityClient.getV2(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                Mockito.eq(incidentUrn),
                any(),
                Mockito.anyBoolean()))
        .thenReturn(new EntityResponse().setUrn(incidentUrn).setAspects(incidentAspects));

    final EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, true, false));

    final List<Health> result = resolver.get(legacyEnv(TEST_DATASET_URN)).get();

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getType(), HealthStatusType.INCIDENTS);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getMessage(), "1 active incident");
    assertEquals(
        result.get(0).getActiveIncidentHealthDetails().getLatestIncidentUrn(),
        incidentUrn.toString());
    assertEquals(
        result.get(0).getActiveIncidentHealthDetails().getLatestIncidentTitle(),
        "nightly load failed");
  }

  /**
   * Legacy path: multiple active incidents whose latest incident has no incidentInfo aspect still
   * yield a failing INCIDENTS health with the correct plural count — the missing info just leaves
   * the title/timestamp unpopulated. Exercises the getIncidentInfo "no aspect" branch.
   */
  @Test
  public void testLegacyActiveIncidentWithMissingInfoStillFails() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    final Urn incidentUrn = Urn.createFromString("urn:li:incident:i2");
    Mockito.when(
            mockEntityClient.filter(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                any(),
                any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenReturn(
            new SearchResult()
                .setNumEntities(2)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(new SearchEntity().setEntity(incidentUrn)))));
    // Incident entity returns no incidentInfo aspect.
    Mockito.when(mockEntityClient.getV2(any(), any(), any(), any(), Mockito.anyBoolean()))
        .thenReturn(new EntityResponse().setUrn(incidentUrn).setAspects(new EnvelopedAspectMap()));

    final EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, true, false));

    final List<Health> result = resolver.get(legacyEnv(TEST_DATASET_URN)).get();

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getMessage(), "2 active incidents");
    assertEquals(
        result.get(0).getActiveIncidentHealthDetails().getLatestIncidentUrn(),
        incidentUrn.toString());
    assertNull(result.get(0).getActiveIncidentHealthDetails().getLatestIncidentTitle());
  }

  /**
   * Legacy path: a dataset with an active assertion whose latest run FAILED resolves to a failing
   * ASSERTIONS health. Exercises computeAssertionHealthForAsset and getAssertionRunsTable (the
   * single-URN getAggregatedStats call, distinct from the batched path).
   */
  @Test
  public void testLegacyFailingAssertionReportsFailingHealth() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(500),
                any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship()
                                .setEntity(Urn.createFromString(TEST_ASSERTION_URN))
                                .setType("Asserts")))));
    Mockito.when(
            mockAspectService.getAggregatedStats(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                any(),
                any(),
                any()))
        .thenReturn(
            new GenericTable()
                .setColumnNames(
                    new StringArray(ImmutableList.of("assertionUrn", "type", "timestampMillis")))
                .setColumnTypes(new StringArray("string", "string", "long"))
                .setRows(
                    new StringArrayArray(
                        ImmutableList.of(
                            new StringArray(
                                ImmutableList.of(TEST_ASSERTION_URN, "FAILURE", "0"))))));

    final EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(true, false, false));

    final List<Health> result = resolver.get(legacyEnv(TEST_DATASET_URN)).get();

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getType(), HealthStatusType.ASSERTIONS);
    assertEquals(result.get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(0).getMessage(), "1 of 1 assertions are failing");
  }

  /**
   * Legacy path resilience: when the incident search backend errors, the incidents dimension
   * degrades to no health rather than failing the whole field. Exercises the
   * computeIncidentsHealthForAsset catch path.
   */
  @Test
  public void testLegacyIncidentBackendFailureDegradesGracefully() throws Exception {
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    TimeseriesAspectService mockAspectService = Mockito.mock(TimeseriesAspectService.class);

    Mockito.when(
            mockEntityClient.filter(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                any(),
                any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenThrow(new RemoteInvocationException("incident index down"));

    final EntityHealthResolver resolver =
        new EntityHealthResolver(
            mockEntityClient,
            mockGraphClient,
            mockAspectService,
            new EntityHealthResolver.Config(false, true, false));

    final List<Health> result = resolver.get(legacyEnv(TEST_DATASET_URN)).get();

    // Incident dimension dropped; the field still resolves (no crash).
    assertEquals(result.size(), 0);
  }

  /** Builds a DataFetchingEnvironment for the legacy (non-batch) resolver path. */
  private static DataFetchingEnvironment legacyEnv(final String datasetUrn) {
    final QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(context.getOperationContext()).thenReturn(Mockito.mock(OperationContext.class));
    final DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getContext()).thenReturn(context);
    final Dataset parentDataset = new Dataset();
    parentDataset.setUrn(datasetUrn);
    Mockito.when(env.getSource()).thenReturn(parentDataset);
    return env;
  }
}
