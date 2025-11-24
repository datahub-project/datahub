package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersInput;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersType;
import com.linkedin.datahub.graphql.generated.AuditLogSpecInput;
import com.linkedin.datahub.graphql.generated.CreateAssertionMonitorInput;
import com.linkedin.datahub.graphql.generated.CronScheduleInput;
import com.linkedin.datahub.graphql.generated.DatasetFieldAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetFieldAssertionSourceType;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessSourceType;
import com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.DatasetVolumeSourceType;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateAssertionMonitorResolverTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,test)", TEST_ENTITY_URN));
  private static final String TEST_EXECUTOR_ID = "testExecutorId";

  private static final MonitorKey TEST_MONITOR_KEY =
      new MonitorKey()
          .setEntity(UrnUtils.getUrn(TEST_MONITOR_URN.getEntityKey().get(0)))
          .setId(TEST_MONITOR_URN.getEntityKey().get(1));

  private static final CreateAssertionMonitorInput TEST_FRESHNESS_INPUT =
      new CreateAssertionMonitorInput(
          TEST_ENTITY_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          new CronScheduleInput("1 * * * *", "America/Los_Angeles"),
          new AssertionEvaluationParametersInput(
              AssertionEvaluationParametersType.DATASET_FRESHNESS,
              new DatasetFreshnessAssertionParametersInput(
                  DatasetFreshnessSourceType.AUDIT_LOG,
                  null,
                  new AuditLogSpecInput(ImmutableList.of("INSERT"), "testUser"),
                  null),
              null,
              null,
              null),
          TEST_EXECUTOR_ID);

  private static final MonitorInfo TEST_MONITOR_INFO_FRESHNESS =
      new MonitorInfo()
          .setType(MonitorType.ASSERTION)
          .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
          .setExecutorId(TEST_EXECUTOR_ID)
          .setAssertionMonitor(
              new AssertionMonitor()
                  .setAssertions(
                      new AssertionEvaluationSpecArray(
                          ImmutableList.of(
                              new AssertionEvaluationSpec()
                                  .setAssertion(TEST_ASSERTION_URN)
                                  .setSchedule(
                                      new CronSchedule()
                                          .setCron("1 * * * *")
                                          .setTimezone("America/Los_Angeles"))
                                  .setParameters(
                                      new AssertionEvaluationParameters()
                                          .setType(
                                              com.linkedin.monitor.AssertionEvaluationParametersType
                                                  .DATASET_FRESHNESS)
                                          .setDatasetFreshnessParameters(
                                              new DatasetFreshnessAssertionParameters()
                                                  .setSourceType(
                                                      com.linkedin.monitor
                                                          .DatasetFreshnessSourceType.AUDIT_LOG)
                                                  .setAuditLog(
                                                      new AuditLogSpec()
                                                          .setOperationTypes(
                                                              new StringArray(
                                                                  ImmutableList.of("INSERT")))
                                                          .setUserName("testUser"))))))));

  private static final CreateAssertionMonitorInput TEST_VOLUME_INPUT =
      new CreateAssertionMonitorInput(
          TEST_ENTITY_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          new CronScheduleInput("1 * * * *", "America/Los_Angeles"),
          new AssertionEvaluationParametersInput(
              AssertionEvaluationParametersType.DATASET_VOLUME,
              null,
              new DatasetVolumeAssertionParametersInput(
                  DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE),
              null,
              null),
          TEST_EXECUTOR_ID);

  private static final MonitorInfo TEST_MONITOR_INFO_VOLUME =
      new MonitorInfo()
          .setType(MonitorType.ASSERTION)
          .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
          .setExecutorId(TEST_EXECUTOR_ID)
          .setAssertionMonitor(
              new AssertionMonitor()
                  .setAssertions(
                      new AssertionEvaluationSpecArray(
                          ImmutableList.of(
                              new AssertionEvaluationSpec()
                                  .setAssertion(TEST_ASSERTION_URN)
                                  .setSchedule(
                                      new CronSchedule()
                                          .setCron("1 * * * *")
                                          .setTimezone("America/Los_Angeles"))
                                  .setParameters(
                                      new AssertionEvaluationParameters()
                                          .setType(
                                              com.linkedin.monitor.AssertionEvaluationParametersType
                                                  .DATASET_VOLUME)
                                          .setDatasetVolumeParameters(
                                              new DatasetVolumeAssertionParameters()
                                                  .setSourceType(
                                                      com.linkedin.monitor.DatasetVolumeSourceType
                                                          .DATAHUB_DATASET_PROFILE)))))));

  private static final CreateAssertionMonitorInput TEST_SQL_INPUT =
      new CreateAssertionMonitorInput(
          TEST_ENTITY_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          new CronScheduleInput("1 * * * *", "America/Los_Angeles"),
          new AssertionEvaluationParametersInput(
              AssertionEvaluationParametersType.DATASET_SQL, null, null, null, null),
          TEST_EXECUTOR_ID);

  private static final MonitorInfo TEST_MONITOR_INFO_SQL =
      new MonitorInfo()
          .setType(MonitorType.ASSERTION)
          .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
          .setExecutorId(TEST_EXECUTOR_ID)
          .setAssertionMonitor(
              new AssertionMonitor()
                  .setAssertions(
                      new AssertionEvaluationSpecArray(
                          ImmutableList.of(
                              new AssertionEvaluationSpec()
                                  .setAssertion(TEST_ASSERTION_URN)
                                  .setSchedule(
                                      new CronSchedule()
                                          .setCron("1 * * * *")
                                          .setTimezone("America/Los_Angeles"))
                                  .setParameters(
                                      new AssertionEvaluationParameters()
                                          .setType(
                                              com.linkedin.monitor.AssertionEvaluationParametersType
                                                  .DATASET_SQL))))));

  private static final CreateAssertionMonitorInput TEST_FIELD_INPUT =
      new CreateAssertionMonitorInput(
          TEST_ENTITY_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          new CronScheduleInput("1 * * * *", "America/Los_Angeles"),
          new AssertionEvaluationParametersInput(
              AssertionEvaluationParametersType.DATASET_FIELD,
              null,
              null,
              new DatasetFieldAssertionParametersInput(
                  DatasetFieldAssertionSourceType.ALL_ROWS_QUERY, null),
              null),
          TEST_EXECUTOR_ID);

  private static final MonitorInfo TEST_MONITOR_INFO_FIELD =
      new MonitorInfo()
          .setType(MonitorType.ASSERTION)
          .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
          .setExecutorId(TEST_EXECUTOR_ID)
          .setAssertionMonitor(
              new AssertionMonitor()
                  .setAssertions(
                      new AssertionEvaluationSpecArray(
                          ImmutableList.of(
                              new AssertionEvaluationSpec()
                                  .setAssertion(TEST_ASSERTION_URN)
                                  .setSchedule(
                                      new CronSchedule()
                                          .setCron("1 * * * *")
                                          .setTimezone("America/Los_Angeles"))
                                  .setParameters(
                                      new AssertionEvaluationParameters()
                                          .setType(
                                              com.linkedin.monitor.AssertionEvaluationParametersType
                                                  .DATASET_FIELD)
                                          .setDatasetFieldParameters(
                                              new DatasetFieldAssertionParameters()
                                                  .setSourceType(
                                                      com.linkedin.monitor
                                                          .DatasetFieldAssertionSourceType
                                                          .ALL_ROWS_QUERY)))))));

  @Test
  public void testGetSuccessFreshnessAssertion() throws Exception {
    // Create resolver
    MonitorService mockService = initMockMonitorService(TEST_MONITOR_INFO_FRESHNESS);
    AssertionService mockAssertionService =
        initMockAssertionsService(TEST_ASSERTION_URN, AssertionType.FRESHNESS);
    CreateAssertionMonitorResolver resolver =
        new CreateAssertionMonitorResolver(mockService, mockAssertionService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_FRESHNESS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(monitor);
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getEntity().getUrn(), TEST_ENTITY_URN.toString());

    // Validate that we created the assertion
    AssertionEvaluationSpec evaluationSpec =
        TEST_MONITOR_INFO_FRESHNESS.getAssertionMonitor().getAssertions().get(0);
    Mockito.verify(mockService, Mockito.times(1))
        .createAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(evaluationSpec.getAssertion()),
            Mockito.eq(evaluationSpec.getSchedule()),
            Mockito.eq(evaluationSpec.getParameters()),
            Mockito.eq(TEST_EXECUTOR_ID),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));
  }

  @Test
  public void testGetSuccessVolumeAssertion() throws Exception {
    // Create resolver
    MonitorService mockService = initMockMonitorService(TEST_MONITOR_INFO_VOLUME);
    AssertionService mockAssertionService =
        initMockAssertionsService(TEST_ASSERTION_URN, AssertionType.VOLUME);
    CreateAssertionMonitorResolver resolver =
        new CreateAssertionMonitorResolver(mockService, mockAssertionService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VOLUME_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(monitor);
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getEntity().getUrn(), TEST_ENTITY_URN.toString());

    // Validate that we created the assertion
    AssertionEvaluationSpec evaluationSpec =
        TEST_MONITOR_INFO_VOLUME.getAssertionMonitor().getAssertions().get(0);
    Mockito.verify(mockService, Mockito.times(1))
        .createAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(evaluationSpec.getAssertion()),
            Mockito.eq(evaluationSpec.getSchedule()),
            Mockito.eq(evaluationSpec.getParameters()),
            Mockito.eq(TEST_EXECUTOR_ID),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));
  }

  @Test
  public void testGetSuccessSqlAssertion() throws Exception {
    // Create resolver
    MonitorService mockService = initMockMonitorService(TEST_MONITOR_INFO_SQL);
    AssertionService mockAssertionService =
        initMockAssertionsService(TEST_ASSERTION_URN, AssertionType.SQL);
    CreateAssertionMonitorResolver resolver =
        new CreateAssertionMonitorResolver(mockService, mockAssertionService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_SQL_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(monitor);
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getEntity().getUrn(), TEST_ENTITY_URN.toString());

    // Validate that we created the assertion
    AssertionEvaluationSpec evaluationSpec =
        TEST_MONITOR_INFO_SQL.getAssertionMonitor().getAssertions().get(0);
    Mockito.verify(mockService, Mockito.times(1))
        .createAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(evaluationSpec.getAssertion()),
            Mockito.eq(evaluationSpec.getSchedule()),
            Mockito.eq(evaluationSpec.getParameters()),
            Mockito.eq(TEST_EXECUTOR_ID),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));

    // Ensure that we retrieved the assertion info to check whether it is of type SQL.
    Mockito.verify(mockAssertionService, Mockito.times(1))
        .getAssertionInfo(any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN));

    // Ensure that we authorized the SQL create.
    Mockito.verify(mockContext.getAuthorizer(), Mockito.times(1))
        .authorize(
            Mockito.eq(
                new AuthorizationRequest(
                    "urn:li:corpuser:test",
                    PoliciesConfig.EDIT_ENTITY_SQL_ASSERTION_MONITORS.getType(),
                    Optional.of(new EntitySpec("dataset", TEST_ENTITY_URN.toString())),
                    Collections.emptyList())));
  }

  @Test
  public void testGetSuccessFieldAssertion() throws Exception {
    // Create resolver
    MonitorService mockService = initMockMonitorService(TEST_MONITOR_INFO_VOLUME);
    AssertionService mockAssertionService =
        initMockAssertionsService(TEST_ASSERTION_URN, AssertionType.FIELD);
    CreateAssertionMonitorResolver resolver =
        new CreateAssertionMonitorResolver(mockService, mockAssertionService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_FIELD_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(monitor);
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getEntity().getUrn(), TEST_ENTITY_URN.toString());

    // Validate that we created the assertion
    AssertionEvaluationSpec evaluationSpec =
        TEST_MONITOR_INFO_FIELD.getAssertionMonitor().getAssertions().get(0);
    Mockito.verify(mockService, Mockito.times(1))
        .createAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(evaluationSpec.getAssertion()),
            Mockito.eq(evaluationSpec.getSchedule()),
            Mockito.eq(evaluationSpec.getParameters()),
            Mockito.eq(TEST_EXECUTOR_ID),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    MonitorService mockService = initMockMonitorService(TEST_MONITOR_INFO_FRESHNESS);
    AssertionService mockAssertionService =
        initMockAssertionsService(TEST_ASSERTION_URN, AssertionType.FRESHNESS);
    CreateAssertionMonitorResolver resolver =
        new CreateAssertionMonitorResolver(mockService, mockAssertionService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_FRESHNESS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testGetAssertionServiceException() throws Exception {
    // Create resolver
    MonitorService mockService = initMockMonitorService(TEST_MONITOR_INFO_FRESHNESS);
    AssertionService mockAssertionService =
        initMockAssertionsService(TEST_ASSERTION_URN, AssertionType.FRESHNESS);

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .createAssertionMonitor(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));

    CreateAssertionMonitorResolver resolver =
        new CreateAssertionMonitorResolver(mockService, mockAssertionService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_FRESHNESS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockMonitorService(MonitorInfo monitorInfo) throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);
    Mockito.when(
            service.createAssertionMonitor(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(Constants.METADATA_TESTS_SOURCE)))
        .thenReturn(TEST_MONITOR_URN);

    Mockito.when(
            service.getMonitorEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_MONITOR_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.MONITOR_KEY_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(TEST_MONITOR_KEY.data())),
                            Constants.MONITOR_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(monitorInfo.data())))))
                .setEntityName(Constants.MONITOR_ENTITY_NAME)
                .setUrn(TEST_MONITOR_URN));
    return service;
  }

  private AssertionService initMockAssertionsService(
      Urn assertionUrn, AssertionType assertionType) {
    AssertionService service = Mockito.mock(AssertionService.class);

    AssertionInfo nonSqlAssertion = new AssertionInfo();
    nonSqlAssertion.setType(assertionType);

    Mockito.when(service.getAssertionInfo(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(nonSqlAssertion);

    return service;
  }
}
