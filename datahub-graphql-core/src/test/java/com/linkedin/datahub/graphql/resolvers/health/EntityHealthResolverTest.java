package com.linkedin.datahub.graphql.resolvers.health;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetHealthResolver;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.GenericTable;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
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
}
