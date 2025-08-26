package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionAdjustmentSettings;
import com.linkedin.assertion.AssertionMonitorSensitivity;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionAdjustmentSettingsInput;
import com.linkedin.datahub.graphql.generated.AssertionMonitorSensitivityInput;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.UpdateAssertionMonitorSettingsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionMonitorSettings;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateAssertionMonitorSettingsResolverTest {

  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(
          String.format(
              "urn:li:monitor:(%s,%s)",
              TEST_ENTITY_URN.toString(), AcrylConstants.FRESHNESS_SYSTEM_MONITOR_ID));

  private static final AssertionAdjustmentSettingsInput TEST_ADJUSTMENT_SETTINGS =
      new AssertionAdjustmentSettingsInput(
          null, // AdjustmentAlgorithm algorithm
          null, // String algorithmName
          null, // List<StringMapEntryInput> context
          null, // List<AssertionExclusionWindowInput> exclusionWindows
          60, // Integer trainingDataLookbackWindowDays
          new AssertionMonitorSensitivityInput(5) // AssertionMonitorSensitivityInput sensitivity
          );

  private static final UpdateAssertionMonitorSettingsInput TEST_INPUT =
      new UpdateAssertionMonitorSettingsInput(
          TEST_MONITOR_URN.toString(), TEST_ADJUSTMENT_SETTINGS);

  // Helper to build a test MonitorInfo
  private static MonitorInfo buildTestMonitorInfo() throws URISyntaxException {
    return new MonitorInfo()
        .setType(MonitorType.ASSERTION)
        .setStatus(new com.linkedin.monitor.MonitorStatus().setMode(MonitorMode.ACTIVE))
        .setAssertionMonitor(
            new AssertionMonitor()
                .setAssertions(
                    new AssertionEvaluationSpecArray(
                        List.of(
                            new AssertionEvaluationSpec()
                                .setSchedule(
                                    new CronSchedule().setCron("* * * * *").setTimezone("UTC"))
                                .setAssertion(
                                    Urn.createFromString("urn:li:assertion:test-assertion")))))
                .setSettings(
                    new AssertionMonitorSettings()
                        .setAdjustmentSettings(
                            new AssertionAdjustmentSettings()
                                .setSensitivity(new AssertionMonitorSensitivity().setLevel(1)))));
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(true);
    UpdateAssertionMonitorSettingsResolver resolver =
        new UpdateAssertionMonitorSettingsResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor result = resolver.get(mockEnv).get();

    // Capture the proposal passed to ingestProposal
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), Mockito.eq(false));

    // Extract and verify the aspect
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), TEST_MONITOR_URN);
    assertEquals(proposal.getAspectName(), "monitorInfo");

    // Build the expected MonitorInfo with the updated settings
    MonitorInfo expectedInfo = buildTestMonitorInfo();
    expectedInfo
        .getAssertionMonitor()
        .getSettings()
        .setAdjustmentSettings(
            new AssertionAdjustmentSettings()
                .setSensitivity(
                    new AssertionMonitorSensitivity()
                        .setLevel(TEST_INPUT.getAdjustmentSettings().getSensitivity().getLevel()))
                .setTrainingDataLookbackWindowDays(
                    TEST_INPUT.getAdjustmentSettings().getTrainingDataLookbackWindowDays()));

    // Build the expected MetadataChangeProposal
    MetadataChangeProposal expectedMcp =
        AspectUtils.buildMetadataChangeProposal(
            TEST_MONITOR_URN, Constants.MONITOR_INFO_ASPECT_NAME, expectedInfo);

    // Compare the actual and expected aspects
    assertEquals(proposal.getAspect(), expectedMcp.getAspect());
  }

  @Test
  public void testGetEntityDoesNotExist() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(false);
    UpdateAssertionMonitorSettingsResolver resolver =
        new UpdateAssertionMonitorSettingsResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    MonitorService mockService = initMockService();
    UpdateAssertionMonitorSettingsResolver resolver =
        new UpdateAssertionMonitorSettingsResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verifyNoMoreInteractions(mockClient);
  }

  @Test
  public void testGetMonitorServiceException() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(true);
    Mockito.doThrow(RuntimeException.class)
        .when(mockClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));

    UpdateAssertionMonitorSettingsResolver resolver =
        new UpdateAssertionMonitorSettingsResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockService() throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);

    // Mock existing monitor info
    MonitorInfo info = buildTestMonitorInfo();

    Mockito.when(service.getMonitorInfo(any(OperationContext.class), Mockito.eq(TEST_MONITOR_URN)))
        .thenReturn(info);

    Mockito.when(
            service.getMonitorEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_MONITOR_URN)))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_MONITOR_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.MONITOR_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.MONITOR_INFO_ASPECT_NAME)
                                .setVersion(0L)
                                .setValue(new Aspect(info.data()))))));

    return service;
  }

  private EntityClient initMockClient(boolean exists) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    if (exists) {
      Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(TEST_ENTITY_URN)))
          .thenReturn(true);
    } else {
      Mockito.when(client.exists(any(OperationContext.class), Mockito.eq(TEST_ENTITY_URN)))
          .thenReturn(false);
    }
    return client;
  }
}
