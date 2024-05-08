package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.*;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionInput;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
import com.linkedin.datahub.graphql.generated.AssertionActionsInput;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.CronScheduleInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterType;
import com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.RowCountTotalInput;
import com.linkedin.datahub.graphql.generated.UpsertDatasetVolumeAssertionMonitorInput;
import com.linkedin.datahub.graphql.generated.VolumeAssertionType;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpsertDatasetVolumeAssertionMonitorResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:actor:test");

  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,test)", TEST_DATASET_URN));

  private static final String TEST_EXECUTOR_ID = "testExecutorId";
  private static final UpsertDatasetVolumeAssertionMonitorInput TEST_INPUT =
      new UpsertDatasetVolumeAssertionMonitorInput(
          TEST_DATASET_URN.toString(),
          "description",
          VolumeAssertionType.ROW_COUNT_TOTAL,
          new RowCountTotalInput(
              AssertionStdOperator.EQUAL_TO,
              new AssertionStdParametersInput(
                  new AssertionStdParameterInput("100", AssertionStdParameterType.NUMBER),
                  null,
                  null)),
          null,
          null,
          null,
          new DatasetFilterInput(DatasetFilterType.SQL, "some_condition = True"),
          new AssertionActionsInput(
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RESOLVE_INCIDENT)),
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RAISE_INCIDENT))),
          new CronScheduleInput("* * * * *", "America / Los Angeles"),
          new DatasetVolumeAssertionParametersInput(
              com.linkedin.datahub.graphql.generated.DatasetVolumeSourceType.QUERY),
          com.linkedin.datahub.graphql.generated.MonitorMode.ACTIVE,
          TEST_EXECUTOR_ID);

  private static final UpsertDatasetVolumeAssertionMonitorInput TEST_CREATE_INPUT =
      new UpsertDatasetVolumeAssertionMonitorInput(
          null,
          "description",
          VolumeAssertionType.ROW_COUNT_TOTAL,
          new RowCountTotalInput(
              AssertionStdOperator.EQUAL_TO,
              new AssertionStdParametersInput(
                  new AssertionStdParameterInput("100", AssertionStdParameterType.NUMBER),
                  null,
                  null)),
          null,
          null,
          null,
          new DatasetFilterInput(DatasetFilterType.SQL, "some_condition = True"),
          new AssertionActionsInput(
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RESOLVE_INCIDENT)),
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RAISE_INCIDENT))),
          new CronScheduleInput("* * * * *", "America / Los Angeles"),
          new DatasetVolumeAssertionParametersInput(
              com.linkedin.datahub.graphql.generated.DatasetVolumeSourceType.QUERY),
          com.linkedin.datahub.graphql.generated.MonitorMode.ACTIVE,
          TEST_EXECUTOR_ID);

  private static final UpsertDatasetVolumeAssertionMonitorInput TEST_UPDATE_INPUT_ENTITY_MISMATCH =
      new UpsertDatasetVolumeAssertionMonitorInput(
          "urn:li:dataset:(urn:li:dataPlatform:hive,another_name,PROD)",
          "description",
          VolumeAssertionType.ROW_COUNT_TOTAL,
          new RowCountTotalInput(
              AssertionStdOperator.EQUAL_TO,
              new AssertionStdParametersInput(
                  new AssertionStdParameterInput("100", AssertionStdParameterType.NUMBER),
                  null,
                  null)),
          null,
          null,
          null,
          new DatasetFilterInput(DatasetFilterType.SQL, "some_condition = True"),
          new AssertionActionsInput(
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RESOLVE_INCIDENT)),
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RAISE_INCIDENT))),
          new CronScheduleInput("* * * * *", "America / Los Angeles"),
          new DatasetVolumeAssertionParametersInput(
              com.linkedin.datahub.graphql.generated.DatasetVolumeSourceType.QUERY),
          com.linkedin.datahub.graphql.generated.MonitorMode.ACTIVE,
          TEST_EXECUTOR_ID);

  private static final AssertionInfo TEST_ASSERTION_INFO =
      new AssertionInfo()
          .setType(AssertionType.VOLUME)
          .setSource(
              new AssertionSource()
                  .setType(AssertionSourceType.NATIVE)
                  .setCreated(
                      new AuditStamp()
                          .setTime(System.currentTimeMillis())
                          .setActor(TEST_ACTOR_URN)))
          .setVolumeAssertion(
              new VolumeAssertionInfo()
                  .setEntity(TEST_DATASET_URN)
                  .setType(com.linkedin.assertion.VolumeAssertionType.ROW_COUNT_TOTAL)
                  .setRowCountTotal(
                      new com.linkedin.assertion.RowCountTotal()
                          .setOperator(com.linkedin.assertion.AssertionStdOperator.EQUAL_TO)
                          .setParameters(
                              new com.linkedin.assertion.AssertionStdParameters()
                                  .setValue(
                                      new com.linkedin.assertion.AssertionStdParameter()
                                          .setValue("100")
                                          .setType(
                                              com.linkedin.assertion.AssertionStdParameterType
                                                  .NUMBER))))
                  .setFilter(
                      new DatasetFilter()
                          .setType(com.linkedin.dataset.DatasetFilterType.SQL)
                          .setSql("some_condition = True")));

  private static final AssertionInfo NON_VOLUME_ASSERTION_INFO =
      new AssertionInfo()
          .setType(AssertionType.FRESHNESS)
          .setFreshnessAssertion(
              new FreshnessAssertionInfo()
                  .setEntity(TEST_DATASET_URN)
                  .setType(FreshnessAssertionType.DATASET_CHANGE)
                  .setSchedule(
                      new FreshnessAssertionSchedule()
                          .setType(com.linkedin.assertion.FreshnessAssertionScheduleType.CRON)
                          .setCron(
                              new FreshnessCronSchedule()
                                  .setCron("* * * * *")
                                  .setTimezone("America / Los Angeles"))));
  private static final AssertionActions TEST_ASSERTION_ACTIONS =
      new AssertionActions()
          .setOnSuccess(
              new AssertionActionArray(
                  ImmutableList.of(
                      new AssertionAction()
                          .setType(com.linkedin.assertion.AssertionActionType.RESOLVE_INCIDENT))))
          .setOnFailure(
              new AssertionActionArray(
                  ImmutableList.of(
                      new AssertionAction()
                          .setType(com.linkedin.assertion.AssertionActionType.RAISE_INCIDENT))));

  private static final AssertionEvaluationSpec evaluationSpec =
      new AssertionEvaluationSpec()
          .setAssertion(TEST_ASSERTION_URN)
          .setSchedule(new CronSchedule().setCron("* * * * *").setTimezone("America / Los Angeles"))
          .setParameters(
              new AssertionEvaluationParameters()
                  .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
                  .setDatasetVolumeParameters(
                      new DatasetVolumeAssertionParameters()
                          .setSourceType(DatasetVolumeSourceType.QUERY)));
  private static final MonitorInfo TEST_MONITOR_INFO =
      new MonitorInfo()
          .setType(MonitorType.ASSERTION)
          .setAssertionMonitor(
              new AssertionMonitor()
                  .setAssertions(
                      new AssertionEvaluationSpecArray(ImmutableList.of(evaluationSpec))))
          .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
          .setExecutorId(TEST_EXECUTOR_ID);

  @Test
  public void testGetSuccessCreateAssertion() throws Exception {
    // Update resolver
    AssertionService assertionService = initMockAssertionService();
    MonitorService monitorService = initMockMonitorService();
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            assertionService, monitorService, graphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assertion assertion = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    // Validate that we created the assertion
    Mockito.verify(assertionService, Mockito.times(1))
        .upsertDatasetVolumeAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_ASSERTION_INFO.getVolumeAssertion().getEntity()),
            Mockito.eq("description"),
            Mockito.eq(TEST_ASSERTION_INFO.getVolumeAssertion()),
            Mockito.eq(TEST_ASSERTION_ACTIONS),
            Mockito.isNull());

    // Validate that we created the monitor
    Mockito.verify(monitorService, Mockito.times(1))
        .upsertAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(TEST_MONITOR_URN),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(evaluationSpec.getSchedule()),
            Mockito.eq(
                TEST_MONITOR_INFO.getAssertionMonitor().getAssertions().get(0).getParameters()),
            Mockito.eq(TEST_MONITOR_INFO.getStatus().getMode()),
            Mockito.eq(TEST_MONITOR_INFO.getExecutorId()));
  }

  @Test
  public void testGetCreateAssertionMonitorFailure() throws Exception {
    // Update resolver
    AssertionService assertionService = initMockAssertionService();
    MonitorService monitorService = initMockMonitorService();
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            assertionService, monitorService, graphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            monitorService.upsertAssertionMonitor(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(evaluationSpec.getSchedule()),
                Mockito.eq(
                    TEST_MONITOR_INFO.getAssertionMonitor().getAssertions().get(0).getParameters()),
                Mockito.eq(TEST_MONITOR_INFO.getStatus().getMode()),
                Mockito.eq(TEST_MONITOR_INFO.getExecutorId())))
        .thenThrow(RemoteInvocationException.class);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we created the assertion
    Mockito.verify(assertionService, Mockito.times(1))
        .upsertDatasetVolumeAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq("description"),
            Mockito.eq(TEST_ASSERTION_INFO.getVolumeAssertion()),
            Mockito.eq(TEST_ASSERTION_ACTIONS),
            Mockito.isNull());

    // Validate that we deleted the assertion
    Mockito.verify(assertionService, Mockito.times(1))
        .tryDeleteAssertion(any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN));
  }

  @Test
  public void testGetCreateAssertionEntityUrnInputAbsent() {
    // Update resolver
    AssertionService assertionService = initMockAssertionService();
    MonitorService monitorService = initMockMonitorService();
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            assertionService, monitorService, graphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_CREATE_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    assertEquals(e.getMessage(), "Failed to create Assertion. entityUrn is required.");
  }

  @Test
  public void testGetUpdateAssertionEntityUrnInputDoesNotMatch() {
    // Update resolver
    AssertionService assertionService = initMockAssertionService();
    MonitorService monitorService = initMockMonitorService();
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            assertionService, monitorService, graphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_UPDATE_INPUT_ENTITY_MISMATCH);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    assertEquals(
        e.getMessage(),
        String.format(
            "Failed to update Assertion. Assertion with urn %s is not linked Entity with urn %s.",
            TEST_ASSERTION_URN, TEST_UPDATE_INPUT_ENTITY_MISMATCH.getEntityUrn()));
  }

  @Test
  public void testGetUpdateNonVolumeAssertionEntity() {
    // Update resolver
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    Mockito.when(
            assertionService.getAssertionInfo(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(NON_VOLUME_ASSERTION_INFO);
    MonitorService monitorService = initMockMonitorService();
    GraphClient graphClient = Mockito.mock(GraphClient.class);
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            assertionService, monitorService, graphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    assertEquals(
        e.getMessage(),
        String.format(
            "Failed to update Assertion. Assertion with urn %s is not a volume assertion.",
            TEST_ASSERTION_URN));
  }

  private MonitorService initMockMonitorService() {
    MonitorService service = Mockito.mock(MonitorService.class);
    Mockito.when(service.generateMonitorUrn(Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(TEST_MONITOR_URN);
    return service;
  }

  @Test
  public void testGetSuccessUpdateAssertion() throws Exception {
    // Update resolver
    AssertionService assertionService = initMockAssertionService();
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    GraphClient graphClient = initMockGraphClient();
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            assertionService, monitorService, graphClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assertion assertion = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    // Validate that we created the assertion
    Mockito.verify(assertionService, Mockito.times(1))
        .upsertDatasetVolumeAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq("description"),
            Mockito.eq(TEST_ASSERTION_INFO.getVolumeAssertion()),
            Mockito.eq(TEST_ASSERTION_ACTIONS),
            Mockito.eq(TEST_ASSERTION_INFO.getSource()));

    // Validate that we created the monitor
    Mockito.verify(monitorService, Mockito.times(1))
        .upsertAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(TEST_MONITOR_URN),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(evaluationSpec.getSchedule()),
            Mockito.eq(evaluationSpec.getParameters()),
            Mockito.eq(MonitorMode.ACTIVE),
            Mockito.eq(TEST_EXECUTOR_ID));
  }

  private GraphClient initMockGraphClient() {
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_MONITOR_URN)))));
    return graphClient;
  }

  @Test
  public void testGetUpdateAssertionUnauthorized() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockService = initMockAssertionService();
    GraphClient graphClient = initMockGraphClient();
    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            mockService, Mockito.mock(MonitorService.class), graphClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage()
        .contains(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testGetUpdateAssertionDoesNotExistException() {
    // Update resolver
    AssertionService mockService = Mockito.mock(AssertionService.class);
    Mockito.when(
            mockService.getAssertionEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(TEST_ASSERTION_URN));

    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            mockService, Mockito.mock(MonitorService.class), Mockito.mock((GraphClient.class)));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    assertEquals(
        e.getMessage(),
        String.format(
            "Failed to update Assertion. Assertion with urn %s does not exist.",
            TEST_ASSERTION_URN));
  }

  @Test
  public void testGetUpdateAssertionMonitorDoesNotExistException() {
    // Update resolver
    AssertionService mockService = initMockAssertionService();
    GraphClient mockClient = Mockito.mock(GraphClient.class);
    Mockito.when(
            mockClient.getRelatedEntities(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setRelationships(new EntityRelationshipArray(ImmutableList.of())));

    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            mockService, Mockito.mock(MonitorService.class), mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RuntimeException e = expectThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    assertEquals(
        e.getMessage(),
        String.format(
            "Failed to upsert Assertion. Monitor for assertion %s does not exist.",
            TEST_ASSERTION_URN));
  }

  @Test
  public void testGetAssertionServiceException() {
    // Update resolver
    AssertionService mockService = initMockAssertionService();
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .upsertDatasetVolumeAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    UpsertDatasetVolumeAssertionMonitorResolver resolver =
        new UpsertDatasetVolumeAssertionMonitorResolver(
            mockService, Mockito.mock(MonitorService.class), initMockGraphClient());

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn")))
        .thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private AssertionService initMockAssertionService() {
    AssertionService service = Mockito.mock(AssertionService.class);

    Mockito.when(service.generateAssertionUrn()).thenReturn(TEST_ASSERTION_URN);

    Mockito.when(
            service.upsertDatasetVolumeAssertion(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(TEST_ASSERTION_URN);

    Mockito.when(
            service.getAssertionEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data())),
                            Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(TEST_ASSERTION_ACTIONS.data())))))
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(TEST_ASSERTION_URN));

    Mockito.when(
            service.getAssertionInfo(any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_ASSERTION_INFO);

    return service;
  }
}
