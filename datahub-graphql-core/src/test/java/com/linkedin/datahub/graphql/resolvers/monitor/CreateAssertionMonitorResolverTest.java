package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class CreateAssertionMonitorResolverTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final Urn TEST_MONITOR_URN = UrnUtils.getUrn(String.format("urn:li:monitor:(%s,test)", TEST_ENTITY_URN));
  private static final String TEST_EXECUTOR_ID = "testExecutorId";

  private static final MonitorKey TEST_MONITOR_KEY = new MonitorKey()
      .setEntity(UrnUtils.getUrn(TEST_MONITOR_URN.getEntityKey().get(0)))
      .setId(TEST_MONITOR_URN.getEntityKey().get(1)
  );

  private static final CreateAssertionMonitorInput TEST_FRESHNESS_INPUT = new CreateAssertionMonitorInput(
      TEST_ENTITY_URN.toString(),
      TEST_ASSERTION_URN.toString(),
      new CronScheduleInput("1 * * * *", "America/Los_Angeles"),
      new AssertionEvaluationParametersInput(
          AssertionEvaluationParametersType.DATASET_FRESHNESS,
          new DatasetFreshnessAssertionParametersInput(
              DatasetFreshnessSourceType.AUDIT_LOG,
              null,
              new AuditLogSpecInput(ImmutableList.of("INSERT"), "testUser"),
              null
          ),
          null
      ),
      TEST_EXECUTOR_ID
  );

  private static final MonitorInfo TEST_MONITOR_INFO_FRESHNESS = new MonitorInfo()
      .setType(MonitorType.ASSERTION)
      .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
      .setExecutorId(TEST_EXECUTOR_ID)
      .setAssertionMonitor(
        new AssertionMonitor()
          .setAssertions(new AssertionEvaluationSpecArray(
              ImmutableList.of(
                  new AssertionEvaluationSpec()
                    .setAssertion(TEST_ASSERTION_URN)
                    .setSchedule(new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles"))
                    .setParameters(new AssertionEvaluationParameters()
                      .setType(com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_FRESHNESS)
                      .setDatasetFreshnessParameters(new DatasetFreshnessAssertionParameters()
                        .setSourceType(com.linkedin.monitor.DatasetFreshnessSourceType.AUDIT_LOG)
                        .setAuditLog(new AuditLogSpec().setOperationTypes(new StringArray(ImmutableList.of("INSERT"))).setUserName("testUser"))
                      )
                  )
              )
          ))
      );

  private static final CreateAssertionMonitorInput TEST_VOLUME_INPUT = new CreateAssertionMonitorInput(
      TEST_ENTITY_URN.toString(),
      TEST_ASSERTION_URN.toString(),
      new CronScheduleInput("1 * * * *", "America/Los_Angeles"),
      new AssertionEvaluationParametersInput(
          AssertionEvaluationParametersType.DATASET_VOLUME,
          null,
          new DatasetVolumeAssertionParametersInput(DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE)
      ),
      TEST_EXECUTOR_ID
  );

  private static final MonitorInfo TEST_MONITOR_INFO_VOLUME = new MonitorInfo()
      .setType(MonitorType.ASSERTION)
      .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
      .setExecutorId(TEST_EXECUTOR_ID)
      .setAssertionMonitor(
          new AssertionMonitor()
              .setAssertions(new AssertionEvaluationSpecArray(
                  ImmutableList.of(
                      new AssertionEvaluationSpec()
                          .setAssertion(TEST_ASSERTION_URN)
                          .setSchedule(new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles"))
                          .setParameters(new AssertionEvaluationParameters()
                              .setType(com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_VOLUME)
                              .setDatasetVolumeParameters(new DatasetVolumeAssertionParameters()
                                  .setSourceType(com.linkedin.monitor.DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE)
                              )
                          )
                  )
              ))
      );

  @Test
  public void testGetSuccessFreshnessAssertion() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(TEST_MONITOR_INFO_FRESHNESS);
    CreateAssertionMonitorResolver resolver = new CreateAssertionMonitorResolver(mockService);

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
    AssertionEvaluationSpec evaluationSpec = TEST_MONITOR_INFO_FRESHNESS.getAssertionMonitor().getAssertions().get(0);
    Mockito.verify(mockService, Mockito.times(1)).createAssertionMonitor(
        Mockito.eq(TEST_ENTITY_URN),
        Mockito.eq(evaluationSpec.getAssertion()),
        Mockito.eq(evaluationSpec.getSchedule()),
        Mockito.eq(evaluationSpec.getParameters()),
        Mockito.eq(TEST_EXECUTOR_ID),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetSuccessVolumeAssertion() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(TEST_MONITOR_INFO_VOLUME);
    CreateAssertionMonitorResolver resolver = new CreateAssertionMonitorResolver(mockService);

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
    AssertionEvaluationSpec evaluationSpec = TEST_MONITOR_INFO_VOLUME.getAssertionMonitor().getAssertions().get(0);
    Mockito.verify(mockService, Mockito.times(1)).createAssertionMonitor(
        Mockito.eq(TEST_ENTITY_URN),
        Mockito.eq(evaluationSpec.getAssertion()),
        Mockito.eq(evaluationSpec.getSchedule()),
        Mockito.eq(evaluationSpec.getParameters()),
        Mockito.eq(TEST_EXECUTOR_ID),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    MonitorService mockService = initMockService(TEST_MONITOR_INFO_FRESHNESS);
    CreateAssertionMonitorResolver resolver = new CreateAssertionMonitorResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_FRESHNESS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetAssertionServiceException() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(TEST_MONITOR_INFO_FRESHNESS);
    Mockito.doThrow(RuntimeException.class).when(mockService).createAssertionMonitor(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class));

    CreateAssertionMonitorResolver resolver = new CreateAssertionMonitorResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_FRESHNESS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockService(MonitorInfo monitorInfo) throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);
    Mockito.when(service.createAssertionMonitor(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class)
    )).thenReturn(TEST_MONITOR_URN);

    Mockito.when(service.getMonitorEntityResponse(
        Mockito.eq(TEST_MONITOR_URN),
        Mockito.any(Authentication.class)
    )).thenReturn(new EntityResponse()
        .setAspects(new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.MONITOR_KEY_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(TEST_MONITOR_KEY.data())),
                Constants.MONITOR_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(monitorInfo.data()))
            )
        ))
        .setEntityName(Constants.MONITOR_ENTITY_NAME)
        .setUrn(TEST_MONITOR_URN)
    );
    return service;
  }
}