package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionAdjustmentSettings;
import com.linkedin.assertion.AssertionMonitorSensitivity;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.SchemaAssertionCompatibility;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionMonitorSettings;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFieldAssertionSourceType;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.monitor.DatasetSchemaAssertionParameters;
import com.linkedin.monitor.DatasetSchemaSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaFieldSpec;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.timeseries.CalendarInterval;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MonitorServiceTest {

  private static final Urn TEST_MONITOR_URN = UrnUtils.getUrn("urn:li:monitor:test");
  private static final Urn TEST_NON_EXISTENT_MONITOR_URN =
      UrnUtils.getUrn("urn:li:monitor:test-non-existent");
  private static final Urn TEST_BAD_STATE_MONITOR_URN = UrnUtils.getUrn("urn:li:monitor:bad-state");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final Urn TEST_CONNECTION_URN = UrnUtils.getUrn("urn:li:dataPlatform:test");
  private static final String TEST_SQL_STATEMENT =
      "SELECT COUNT(*) FROM test_db.public.test_table;";
  private static final String TEST_HOST = "localhost";
  private static final Integer TEST_PORT = 9004;

  @Mock private BackoffPolicy backoffPolicy;

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @Test
  public void testGetMonitorInfo() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);
    // Case 1: Info exists
    MonitorInfo info = service.getMonitorInfo(mock(OperationContext.class), TEST_MONITOR_URN);
    Assert.assertEquals(info, mockMonitorInfo());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.MONITOR_ENTITY_NAME),
            Mockito.eq(TEST_MONITOR_URN),
            Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME)));

    // Case 2: Info does not exist
    info = service.getMonitorInfo(mock(OperationContext.class), TEST_NON_EXISTENT_MONITOR_URN);
    Assert.assertNull(info);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.MONITOR_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_MONITOR_URN),
            Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME)));
  }

  @Test
  public void testCreateAssertionMonitorRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));

    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(entityUrn)))
        .thenReturn(true);

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), MONITOR_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              MonitorInfo newMonitorInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      MonitorInfo.class);
              Assert.assertEquals(newMonitorInfo.getType(), MonitorType.ASSERTION);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getAssertion(),
                  assertionUrn);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getSchedule(),
                  schedule);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getParameters(),
                  parameters);
              Assert.assertEquals(newMonitorInfo.getStatus().getMode(), MonitorMode.ACTIVE);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    // Test method
    Urn result =
        service.createAssertionMonitor(
            mock(OperationContext.class), entityUrn, assertionUrn, schedule, parameters, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "monitor");
    Assert.assertEquals(result.getEntityKey().get(0), TEST_ENTITY_URN.toString());
  }

  @Test
  public void testCreateAssertionMonitorRequiredFieldsAssertionDoesNotExist() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    // Method should throw because the assertion does not exist.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.createAssertionMonitor(
                mock(OperationContext.class), entityUrn, assertionUrn, schedule, parameters, null));
  }

  @Test
  public void testCreateAssertionMonitorRequiredFieldsEntityDoesNotExist() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(entityUrn)))
        .thenReturn(false);

    // Method should throw because the assertion does not exist.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.createAssertionMonitor(
                mock(OperationContext.class), entityUrn, assertionUrn, schedule, parameters, null));
  }

  @Test
  public void testUpsertAssertionMonitorRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));
    MonitorMode monitorMode = MonitorMode.ACTIVE;

    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(monitorUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(entityUrn)))
        .thenReturn(true);
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), MONITOR_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              MonitorInfo newMonitorInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      MonitorInfo.class);
              Assert.assertEquals(newMonitorInfo.getType(), MonitorType.ASSERTION);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getAssertion(),
                  assertionUrn);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getSchedule(),
                  schedule);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getParameters(),
                  parameters);
              Assert.assertEquals(newMonitorInfo.getStatus().getMode(), monitorMode);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    // Test method
    Urn result =
        service.upsertAssertionMonitor(
            mock(OperationContext.class),
            monitorUrn,
            assertionUrn,
            entityUrn,
            schedule,
            parameters,
            monitorMode,
            null);

    // Assert result
    Assert.assertEquals(result, TEST_MONITOR_URN);
  }

  @Test
  public void testUpsertAssertionMonitorAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));
    MonitorMode monitorMode = MonitorMode.ACTIVE;
    String executorId = "testExecutorId";

    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(monitorUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(entityUrn)))
        .thenReturn(true);
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), MONITOR_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              MonitorInfo newMonitorInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      MonitorInfo.class);
              Assert.assertEquals(newMonitorInfo.getType(), MonitorType.ASSERTION);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getAssertion(),
                  assertionUrn);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getSchedule(),
                  schedule);
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor().getAssertions().get(0).getParameters(),
                  parameters);
              Assert.assertEquals(newMonitorInfo.getStatus().getMode(), monitorMode);
              Assert.assertEquals(newMonitorInfo.getExecutorId(), executorId);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    // Test method
    Urn result =
        service.upsertAssertionMonitor(
            mock(OperationContext.class),
            monitorUrn,
            assertionUrn,
            entityUrn,
            schedule,
            parameters,
            monitorMode,
            executorId);

    // Assert result
    Assert.assertEquals(result, TEST_MONITOR_URN);
  }

  @Test
  public void testUpsertAssertionMonitorAssertionDoesNotExist() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    // Method should throw because the assertion does not exist.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.upsertAssertionMonitor(
                mock(OperationContext.class),
                monitorUrn,
                assertionUrn,
                entityUrn,
                schedule,
                parameters,
                MonitorMode.ACTIVE,
                null));
  }

  @Test
  public void testUpsertAssertionMonitorEntityDoesNotExist() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                    .setAuditLog(
                        new AuditLogSpec()
                            .setUserName("test")
                            .setOperationTypes(new StringArray())));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(entityUrn)))
        .thenReturn(false);

    // Method should throw because the assertion does not exist.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.upsertAssertionMonitor(
                mock(OperationContext.class),
                monitorUrn,
                assertionUrn,
                entityUrn,
                schedule,
                parameters,
                MonitorMode.ACTIVE,
                null));
  }

  @Test
  public void testUpsertAssertionMonitorBadState() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_BAD_STATE_MONITOR_URN;

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(assertionUrn)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(any(OperationContext.class), Mockito.eq(entityUrn)))
        .thenReturn(true);

    // Method should throw because the monitor is in bad state.
    Exception e =
        Assert.expectThrows(
            Exception.class,
            () ->
                service.upsertAssertionMonitor(
                    mock(OperationContext.class),
                    monitorUrn,
                    assertionUrn,
                    entityUrn,
                    new CronSchedule(),
                    new AssertionEvaluationParameters(),
                    MonitorMode.ACTIVE,
                    null));
    assertEquals(
        e.getMessage(),
        String.format(
            "Failed to update Assertion Monitor. Monitor with urn %s is not linked to any Assertion.",
            monitorUrn, assertionUrn));
  }

  @Test
  private void testUpsertMonitorMode() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(OpenApiClient.class), opContext);

    // Case 1: Info exists
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), MONITOR_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              MonitorInfo newMonitorInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      MonitorInfo.class);
              Assert.assertEquals(newMonitorInfo.getType(), MonitorType.ASSERTION);
              Assert.assertEquals(newMonitorInfo.getStatus().getMode(), MonitorMode.ACTIVE);
              Assert.assertEquals(newMonitorInfo.getAssertionMonitor().getAssertions().size(), 1);

              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));
    Urn urn =
        service.upsertMonitorMode(
            mock(OperationContext.class), TEST_MONITOR_URN, MonitorMode.ACTIVE);
    Assert.assertEquals(urn, TEST_MONITOR_URN);

    // Case 2: Info does not exist
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), MONITOR_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              MonitorInfo newMonitorInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      MonitorInfo.class);
              Assert.assertEquals(newMonitorInfo.getType(), MonitorType.ASSERTION);
              Assert.assertEquals(
                  newMonitorInfo.getStatus().getMode(),
                  MonitorMode.INACTIVE); // Should be in inactive mode.
              Assert.assertEquals(
                  newMonitorInfo.getAssertionMonitor(),
                  new AssertionMonitor()
                      .setAssertions(new AssertionEvaluationSpecArray(Collections.emptyList())));
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));
    urn =
        service.upsertMonitorMode(
            mock(OperationContext.class), TEST_NON_EXISTENT_MONITOR_URN, MonitorMode.INACTIVE);
    Assert.assertEquals(urn, TEST_NON_EXISTENT_MONITOR_URN);
  }

  private static SystemEntityClient createMockEntityClient() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);

    // Init for monitor info
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_MONITOR_URN)
                .setEntityName(MONITOR_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            MONITOR_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockMonitorInfo().data()))))));
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_NON_EXISTENT_MONITOR_URN),
                Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_NON_EXISTENT_MONITOR_URN)
                .setEntityName(MONITOR_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_BAD_STATE_MONITOR_URN),
                Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_BAD_STATE_MONITOR_URN)
                .setEntityName(MONITOR_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            MONITOR_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockBadMonitorInfo().data()))))));

    return mockClient;
  }

  private static MonitorInfo mockMonitorInfo() throws Exception {
    final MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(
        new AssertionMonitor()
            .setAssertions(
                new AssertionEvaluationSpecArray(
                    ImmutableList.of(
                        new AssertionEvaluationSpec()
                            .setAssertion(TEST_ASSERTION_URN)
                            .setSchedule(
                                new CronSchedule()
                                    .setCron("* * * * *")
                                    .setTimezone("America/Los_Angeles"))
                            .setParameters(
                                new AssertionEvaluationParameters()
                                    .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
                                    .setDatasetFreshnessParameters(
                                        new DatasetFreshnessAssertionParameters()
                                            .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG)
                                            .setAuditLog(
                                                new AuditLogSpec()
                                                    .setOperationTypes(new StringArray())
                                                    .setUserName("test"))))))));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }

  private static MonitorInfo mockBadMonitorInfo() throws Exception {
    final MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(new AssertionMonitor());
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }

  @Test
  public void testFreshnessAssertionSuccess() throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            httpClient,
            backoffPolicy,
            3,
            Mockito.mock(OpenApiClient.class),
            opContext);

    final FreshnessAssertionInfo freshnessAssertionInfo =
        new FreshnessAssertionInfo()
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setEntity(TEST_ENTITY_URN)
            .setSchedule(
                new FreshnessAssertionSchedule()
                    .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
                    .setFixedInterval(
                        new FixedIntervalSchedule().setUnit(CalendarInterval.HOUR).setMultiple(1)))
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True"));

    final AssertionEvaluationParameters freshnessParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG));

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    String expectedJson =
        "{\"type\": \"SUCCESS\", \"rowCount\": null, \"missingCount\": null, \"unexpectedCount\": null, "
            + "\"actualAggValue\": null, \"nativeResults\": {     \"Value\": \"200\" }, \"externalUrl\": null, \"error\": null}";
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    AssertionResult result =
        service.testFreshnessAssertion(
            TEST_ENTITY_URN, TEST_CONNECTION_URN, freshnessAssertionInfo, freshnessParameters);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:9004/assertions/evaluate_assertion",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(AssertionResultType.SUCCESS, result.getType());
  }

  @Test
  public void testSqlAssertionSuccess() throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            httpClient,
            backoffPolicy,
            3,
            Mockito.mock(OpenApiClient.class),
            opContext);

    final SqlAssertionInfo sqlAssertionInfo =
        new SqlAssertionInfo()
            .setType(SqlAssertionType.METRIC)
            .setEntity(TEST_ENTITY_URN)
            .setStatement(TEST_SQL_STATEMENT)
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setValue("10")
                            .setType(AssertionStdParameterType.NUMBER)));

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    String expectedJson =
        "{\"type\": \"SUCCESS\", \"rowCount\": null, \"missingCount\": null, \"unexpectedCount\": null, "
            + "\"actualAggValue\": null, \"nativeResults\": {     \"Value\": \"200\" }, \"externalUrl\": null, \"error\": null}";
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    AssertionResult result =
        service.testSqlAssertion(TEST_ENTITY_URN, TEST_CONNECTION_URN, sqlAssertionInfo);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:9004/assertions/evaluate_assertion",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(AssertionResultType.SUCCESS, result.getType());
  }

  @Test
  public void testFieldAssertionSuccess() throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            httpClient,
            backoffPolicy,
            3,
            Mockito.mock(OpenApiClient.class),
            opContext);

    final FieldAssertionInfo fieldAssertionInfo =
        new FieldAssertionInfo()
            .setType(FieldAssertionType.FIELD_METRIC)
            .setEntity(TEST_ENTITY_URN)
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True"))
            .setFieldMetricAssertion(
                new FieldMetricAssertion()
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("test_field")
                            .setType("INTEGER")
                            .setNativeType("INTEGER"))
                    .setMetric(FieldMetricType.UNIQUE_COUNT)
                    .setOperator(AssertionStdOperator.GREATER_THAN)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setValue("10")
                                    .setType(AssertionStdParameterType.NUMBER))));

    final AssertionEvaluationParameters fieldParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FIELD)
            .setDatasetFieldParameters(
                new DatasetFieldAssertionParameters()
                    .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY));

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    String expectedJson =
        "{\"type\": \"SUCCESS\", \"rowCount\": null, \"missingCount\": null, \"unexpectedCount\": null, "
            + "\"actualAggValue\": null, \"nativeResults\": {     \"Value\": \"200\" }, \"externalUrl\": null, \"error\": null}";
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    AssertionResult result =
        service.testFieldAssertion(
            TEST_ENTITY_URN, TEST_CONNECTION_URN, fieldAssertionInfo, fieldParameters);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:9004/assertions/evaluate_assertion",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(AssertionResultType.SUCCESS, result.getType());
  }

  @Test
  public void testSchemaAssertionSuccess() throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            httpClient,
            backoffPolicy,
            3,
            Mockito.mock(OpenApiClient.class),
            opContext);

    final SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setVersion(0);
    schemaMetadata.setHash("testHash");
    schemaMetadata.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new OtherSchema()));
    schemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setFieldPath("testPath")
                    .setType(
                        new SchemaFieldDataType()
                            .setType(SchemaFieldDataType.Type.create(new StringType())))
                    .setNativeDataType("varchar"))));

    final SchemaAssertionInfo schemaAssertionInfo =
        new SchemaAssertionInfo()
            .setCompatibility(SchemaAssertionCompatibility.SUPERSET)
            .setSchema(schemaMetadata);

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    String expectedJson =
        "{\"type\": \"SUCCESS\", \"rowCount\": null, \"missingCount\": null, \"unexpectedCount\": null, "
            + "\"actualAggValue\": null, \"nativeResults\": {     \"Value\": \"200\" }, \"externalUrl\": null, \"error\": null}";
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    AssertionResult result =
        service.testSchemaAssertion(
            TEST_ENTITY_URN,
            TEST_CONNECTION_URN,
            schemaAssertionInfo,
            new AssertionEvaluationParameters()
                .setType(AssertionEvaluationParametersType.DATASET_SCHEMA)
                .setDatasetSchemaParameters(
                    new DatasetSchemaAssertionParameters()
                        .setSourceType(DatasetSchemaSourceType.DATAHUB_SCHEMA)));

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:9004/assertions/evaluate_assertion",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(AssertionResultType.SUCCESS, result.getType());
  }

  @Test
  public void testRunAssertionsSuccess() throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            httpClient,
            backoffPolicy,
            3,
            Mockito.mock(OpenApiClient.class),
            opContext);

    final Urn testUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    final Urn testUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");
    final Urn testUrn3 = UrnUtils.getUrn("urn:li:assertion:test3");
    final List<Urn> urns = ImmutableList.of(testUrn1, testUrn2, testUrn3);

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    String expectedJson =
        "{\"results\": [{ \"urn\": \"urn:li:assertion:test\", \"result\": { \"type\": \"SUCCESS\" }}, { \"urn\": \"urn:li:assertion:test2\", \"result\": { \"type\": \"FAILURE\" }}, { \"urn\": \"urn:li:assertion:test3\", \"result\": { \"type\": \"ERROR\" }}]}";
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    Map<Urn, AssertionResult> result =
        service.runAssertions(urns, true, Collections.emptyMap(), false);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:9004/assertions/evaluate_assertion_urns",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(result.size(), 3);
    assertTrue(result.containsKey(testUrn1));
    assertTrue(result.containsKey(testUrn2));
    assertTrue(result.containsKey(testUrn3));
    assertEquals(result.get(testUrn1).getType(), AssertionResultType.SUCCESS);
    assertEquals(result.get(testUrn2).getType(), AssertionResultType.FAILURE);
    assertEquals(result.get(testUrn3).getType(), AssertionResultType.ERROR);
  }

  @Test
  public void testRunAssertionsAsync() throws Exception {
    final SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            httpClient,
            backoffPolicy,
            3,
            Mockito.mock(OpenApiClient.class),
            opContext);

    final Urn testUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    final Urn testUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");
    final Urn testUrn3 = UrnUtils.getUrn("urn:li:assertion:test3");
    final List<Urn> urns = ImmutableList.of(testUrn1, testUrn2, testUrn3);

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    when(mockResponse.getEntity()).thenReturn(null);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    Map<Urn, AssertionResult> result =
        service.runAssertions(urns, true, Collections.emptyMap(), false);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:9004/assertions/evaluate_assertion_urns",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertNull(result);
  }

  @Test
  public void testShouldRetrainMonitor() throws Exception {
    // Create test service
    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mock(SystemEntityClient.class),
            mock(CloseableHttpClient.class),
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    // Get access to private method using reflection
    java.lang.reflect.Method shouldRetrainMethod =
        MonitorService.class.getDeclaredMethod(
            "shouldRetrainMonitor", MonitorInfo.class, AssertionMonitorSettings.class);
    shouldRetrainMethod.setAccessible(true);

    // Case 1: When existingInfo is null, should not retrain
    boolean result1 =
        (boolean) shouldRetrainMethod.invoke(service, null, new AssertionMonitorSettings());
    assertFalse(result1);

    // Case 2: When existingMonitor has no assertionMonitor, should not retrain
    MonitorInfo infoNoAssertionMonitor =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    boolean result2 =
        (boolean)
            shouldRetrainMethod.invoke(
                service, infoNoAssertionMonitor, new AssertionMonitorSettings());
    assertFalse(result2);

    // Case 3: When existingSettings is null but new settings is not null, but no inference
    // settings, should not retrain
    MonitorInfo infoNoSettings =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(new AssertionMonitor());
    boolean result3 =
        (boolean)
            shouldRetrainMethod.invoke(service, infoNoSettings, new AssertionMonitorSettings());
    assertFalse(result3);

    // Case 4: When existingSettings is not null but new settings is null, should not retrain
    AssertionMonitorSettings existingSettings =
        new AssertionMonitorSettings()
            .setAdjustmentSettings(
                new AssertionAdjustmentSettings()
                    .setSensitivity(new AssertionMonitorSensitivity().setLevel(5)));
    MonitorInfo infoWithSettings =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(new AssertionMonitor().setSettings(existingSettings));
    boolean result4 = (boolean) shouldRetrainMethod.invoke(service, infoWithSettings, null);
    assertFalse(result4);

    // Case 5: When settings are the same, should not retrain
    boolean result5 =
        (boolean) shouldRetrainMethod.invoke(service, infoWithSettings, existingSettings);
    assertFalse(result5);

    // Case 6: When settings are different, should retrain
    AssertionMonitorSettings differentSettings =
        new AssertionMonitorSettings()
            .setAdjustmentSettings(
                new AssertionAdjustmentSettings()
                    .setSensitivity(new AssertionMonitorSensitivity().setLevel(6)));

    boolean result6 =
        (boolean) shouldRetrainMethod.invoke(service, infoWithSettings, differentSettings);
    assertTrue(result6);

    // Case 7: When new assertion adjustment settings, but null existing settings, should retrain.
    AssertionMonitorSettings newSettings =
        new AssertionMonitorSettings()
            .setAdjustmentSettings(
                new AssertionAdjustmentSettings()
                    .setSensitivity(new AssertionMonitorSensitivity().setLevel(6)));

    boolean result7 = (boolean) shouldRetrainMethod.invoke(service, infoNoSettings, newSettings);
    assertTrue(result7);
  }

  @Test
  public void testRetrainMonitorSuccess() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    // Mock HTTP response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);

    // Execute
    service.retrainAssertionMonitor(TEST_MONITOR_URN);

    // Verify
    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockHttpClient).execute(requestCaptor.capture());

    HttpPost capturedRequest = requestCaptor.getValue();
    assertEquals(
        TEST_HOST + ":" + TEST_PORT + "/assertions/train_assertion_monitor",
        capturedRequest.getURI().getAuthority() + capturedRequest.getURI().getPath());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testRetrainMonitorServerError() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    // Mock HTTP response with 500 error
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine =
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 500, "Internal Server Error");
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);

    // Execute - should throw RuntimeException
    service.retrainAssertionMonitor(TEST_MONITOR_URN);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testRetrainMonitorConnectionFailure() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            // Set retryCount to 1 to fail faster in test
            1,
            mock(OpenApiClient.class),
            opContext);

    // Mock HTTP client to throw an exception
    when(mockHttpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new java.io.IOException("Connection refused"));

    // Execute - should throw RuntimeException after retries
    service.retrainAssertionMonitor(TEST_MONITOR_URN);
  }

  @Test
  public void testUpsertAssertionMonitorNoRetrainingRequired() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    MonitorService spyService = spy(service);

    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("0 0 * * * ?").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    MonitorMode mode = MonitorMode.ACTIVE;

    // Mock validateEntity to do nothing
    doNothing().when(spyService).validateEntity(any(OperationContext.class), any(Urn.class));

    // Mock existing monitor info with NO settings
    MonitorInfo existingInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(
                new AssertionMonitor()
                    .setAssertions(
                        new AssertionEvaluationSpecArray(
                            List.of(new AssertionEvaluationSpec().setAssertion(assertionUrn)))));

    doReturn(existingInfo)
        .when(spyService)
        .getMonitorInfo(any(OperationContext.class), eq(monitorUrn));

    // Execute
    Urn result =
        spyService.upsertAssertionMonitor(
            mock(OperationContext.class),
            monitorUrn,
            assertionUrn,
            entityUrn,
            schedule,
            parameters,
            mode,
            null,
            null);

    // Verify
    assertNotNull(result);
    assertEquals(result.toString(), TEST_MONITOR_URN.toString());

    // Verify that batchIngestProposals was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));

    // Verify that retrainMonitor was NOT called
    verify(spyService, never()).retrainAssertionMonitor(any(Urn.class));
  }

  @Test
  public void testUpsertAssertionMonitorRetrainingRequired() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    MonitorService spyService = spy(service);

    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("0 0 * * * ?").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    MonitorMode mode = MonitorMode.ACTIVE;

    // Create existing settings
    AssertionMonitorSettings existingSettings =
        new AssertionMonitorSettings()
            .setAdjustmentSettings(
                new AssertionAdjustmentSettings()
                    .setSensitivity(new AssertionMonitorSensitivity().setLevel(5)));

    // Create new settings with different values
    AssertionMonitorSettings newSettings =
        new AssertionMonitorSettings()
            .setAdjustmentSettings(
                new AssertionAdjustmentSettings()
                    .setSensitivity(new AssertionMonitorSensitivity().setLevel(6)));

    // Mock validateEntity to do nothing
    doNothing().when(spyService).validateEntity(any(OperationContext.class), any(Urn.class));

    // Mock existing monitor info with settings
    MonitorInfo existingInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(
                new AssertionMonitor()
                    .setAssertions(
                        new AssertionEvaluationSpecArray(
                            List.of(new AssertionEvaluationSpec().setAssertion(assertionUrn))))
                    .setSettings(existingSettings));

    doReturn(existingInfo)
        .when(spyService)
        .getMonitorInfo(any(OperationContext.class), eq(monitorUrn));

    // Mock HTTP response for retrainMonitor
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);

    // Execute
    Urn result =
        spyService.upsertAssertionMonitor(
            mock(OperationContext.class),
            monitorUrn,
            assertionUrn,
            entityUrn,
            schedule,
            parameters,
            mode,
            null,
            newSettings);

    // Sleep a short time to allow the async operation to complete
    Thread.sleep(500);

    // Verify
    assertNotNull(result);
    assertEquals(result.toString(), TEST_MONITOR_URN.toString());

    // Verify that batchIngestProposals was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));

    // Verify that retrainMonitor was called
    verify(spyService, times(1)).retrainAssertionMonitor(eq(monitorUrn));
  }

  @Test
  public void testUpsertAssertionMonitorNewSettingsNoExistingSettings() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    MonitorService spyService = spy(service);

    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("0 0 * * * ?").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    MonitorMode mode = MonitorMode.ACTIVE;

    // Create new settings
    AssertionMonitorSettings newSettings =
        new AssertionMonitorSettings()
            .setAdjustmentSettings(
                new AssertionAdjustmentSettings()
                    .setSensitivity(new AssertionMonitorSensitivity().setLevel(6)));

    // Mock validateEntity to do nothing
    doNothing().when(spyService).validateEntity(any(OperationContext.class), any(Urn.class));

    // Mock existing monitor info with NO settings
    MonitorInfo existingInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(
                new AssertionMonitor()
                    .setAssertions(
                        new AssertionEvaluationSpecArray(
                            List.of(new AssertionEvaluationSpec().setAssertion(assertionUrn)))));
    // no settings

    doReturn(existingInfo)
        .when(spyService)
        .getMonitorInfo(any(OperationContext.class), eq(monitorUrn));

    // Execute
    Urn result =
        spyService.upsertAssertionMonitor(
            mock(OperationContext.class),
            monitorUrn,
            assertionUrn,
            entityUrn,
            schedule,
            parameters,
            mode,
            null,
            newSettings);

    // Sleep to allow the async operation to complete (increased for CI environments)
    Thread.sleep(500);

    // Verify
    assertNotNull(result);
    assertEquals(result.toString(), TEST_MONITOR_URN.toString());

    // Verify that batchIngestProposals was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));

    // Verify that retrainMonitor was called
    verify(spyService, times(1)).retrainAssertionMonitor(eq(monitorUrn));
  }

  @Test
  public void testUpsertAssertionMonitorNoExistingMonitor() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);

    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            opContext);

    MonitorService spyService = spy(service);

    Urn entityUrn = TEST_ENTITY_URN;
    Urn assertionUrn = TEST_ASSERTION_URN;
    Urn monitorUrn = TEST_MONITOR_URN;
    CronSchedule schedule =
        new CronSchedule().setCron("0 0 * * * ?").setTimezone("America/Los_Angeles");
    AssertionEvaluationParameters parameters = new AssertionEvaluationParameters();
    MonitorMode mode = MonitorMode.ACTIVE;

    // Mock validateEntity to do nothing
    doNothing().when(spyService).validateEntity(any(OperationContext.class), any(Urn.class));

    // Mock that no existing monitor exists
    doReturn(null).when(spyService).getMonitorInfo(any(OperationContext.class), eq(monitorUrn));

    // Execute
    Urn result =
        spyService.upsertAssertionMonitor(
            mock(OperationContext.class),
            monitorUrn,
            assertionUrn,
            entityUrn,
            schedule,
            parameters,
            mode,
            null,
            null);

    // Verify
    assertNotNull(result);
    assertEquals(result.toString(), TEST_MONITOR_URN.toString());

    // Verify that batchIngestProposals was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));

    // Verify that retrainMonitor was NOT called since there's no previous monitor
    verify(spyService, never()).retrainAssertionMonitor(any(Urn.class));
  }

  @Test
  public void testExecuteRequestWithExceptionMetrics() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    OperationContext mockOpContext = mock(OperationContext.class);
    MetricUtils mockMetricUtils = mock(MetricUtils.class);

    // Mock the operation context to return metric utils
    when(mockOpContext.getMetricUtils()).thenReturn(Optional.of(mockMetricUtils));

    // Create service with mocked dependencies
    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3, // retry count
            mock(OpenApiClient.class),
            mockOpContext);

    // Create a specific exception type to test
    IOException testException = new IOException("Connection refused");

    // Mock HTTP client to throw exception on all attempts
    when(mockHttpClient.execute(any(HttpUriRequest.class))).thenThrow(testException);

    // Execute the request and expect it to fail after retries
    try {
      service.runAssertions(List.of(TEST_ASSERTION_URN), true, Collections.emptyMap(), false);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Expected exception after all retries exhausted
      assertTrue(e.getMessage().contains("Received exception while attempting to run assertions!"));
    }

    // Verify that the metric was incremented for each retry attempt
    // With retry count of 3, we expect 3 increments
    verify(mockMetricUtils, times(3))
        .increment(
            eq(MonitorService.class),
            eq("exception" + MetricUtils.DELIMITER + "ioexception"),
            eq(1d));

    // Verify HTTP client was called 3 times (initial + 2 retries)
    verify(mockHttpClient, times(3)).execute(any(HttpUriRequest.class));
  }

  @Test
  public void testExecuteRequestWithDifferentExceptionTypes() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    OperationContext mockOpContext = mock(OperationContext.class);
    MetricUtils mockMetricUtils = mock(MetricUtils.class);

    // Mock the operation context to return metric utils
    when(mockOpContext.getMetricUtils()).thenReturn(Optional.of(mockMetricUtils));

    // Create service with retry count of 2
    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            2, // retry count
            mock(OpenApiClient.class),
            mockOpContext);

    // Mock HTTP client to throw different exceptions on each attempt
    when(mockHttpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new SocketTimeoutException("Timeout"))
        .thenThrow(new ConnectException("Connection refused"));

    // Execute testSqlAssertion - it returns null on failure instead of throwing
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    AssertionResult result =
        service.testSqlAssertion(
            TEST_ENTITY_URN,
            TEST_CONNECTION_URN,
            new SqlAssertionInfo()
                .setChangeType(AssertionValueChangeType.ABSOLUTE)
                .setType(SqlAssertionType.METRIC)
                .setOperator(AssertionStdOperator.EQUAL_TO)
                .setParameters(parameters)
                .setEntity(TEST_ENTITY_URN)
                .setStatement("SELECT COUNT(*) FROM table WHERE some_condition = True"));

    // Verify result is null due to exception
    assertNull(result);

    // Verify metrics were incremented with correct exception class names
    verify(mockMetricUtils, times(1))
        .increment(
            eq(MonitorService.class),
            eq("exception" + MetricUtils.DELIMITER + "sockettimeoutexception"),
            eq(1d));

    verify(mockMetricUtils, times(1))
        .increment(
            eq(MonitorService.class),
            eq("exception" + MetricUtils.DELIMITER + "connectexception"),
            eq(1d));

    // Verify HTTP client was called twice
    verify(mockHttpClient, times(2)).execute(any(HttpUriRequest.class));
  }

  @Test
  public void testExecuteRequestSuccessAfterRetry() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    OperationContext mockOpContext = mock(OperationContext.class);
    MetricUtils mockMetricUtils = mock(MetricUtils.class);

    // Mock the operation context to return metric utils
    when(mockOpContext.getMetricUtils()).thenReturn(Optional.of(mockMetricUtils));

    // Create service
    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            3,
            mock(OpenApiClient.class),
            mockOpContext);

    // Mock successful response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    String expectedJson = "{\"type\": \"SUCCESS\"}";
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);

    // Mock HTTP client to fail once then succeed
    when(mockHttpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new IOException("Temporary failure"))
        .thenReturn(mockResponse);

    // Execute the request - should succeed after retry
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    AssertionResult result =
        service.testSqlAssertion(
            TEST_ENTITY_URN,
            TEST_CONNECTION_URN,
            new SqlAssertionInfo()
                .setChangeType(AssertionValueChangeType.ABSOLUTE)
                .setType(SqlAssertionType.METRIC)
                .setOperator(AssertionStdOperator.EQUAL_TO)
                .setParameters(parameters)
                .setEntity(TEST_ENTITY_URN)
                .setStatement("SELECT COUNT(*) FROM table WHERE some_condition = True"));

    // Verify result
    assertNotNull(result);
    assertEquals(AssertionResultType.SUCCESS, result.getType());

    // Verify metric was incremented once for the first failure
    verify(mockMetricUtils, times(1))
        .increment(
            eq(MonitorService.class),
            eq("exception" + MetricUtils.DELIMITER + "ioexception"),
            eq(1d));

    // Verify HTTP client was called twice (initial fail + successful retry)
    verify(mockHttpClient, times(2)).execute(any(HttpUriRequest.class));
  }

  @Test
  public void testExecuteRequestNoMetricUtils() throws Exception {
    // Set up mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    OperationContext mockOpContext = mock(OperationContext.class);

    // Mock the operation context to return empty metric utils
    when(mockOpContext.getMetricUtils()).thenReturn(Optional.empty());

    // Create service
    MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            mockHttpClient,
            new ExponentialBackoff(2),
            2,
            mock(OpenApiClient.class),
            mockOpContext);

    // Mock HTTP client to throw exception
    when(mockHttpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new IOException("Connection error"));

    // Execute the request and expect it to fail
    try {
      service.retrainAssertionMonitor(TEST_MONITOR_URN);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Expected exception
      assertTrue(
          e.getMessage().contains("Caught exception while attempting to train assertion monitor!"));
    }

    // Verify getMetricUtils was called but no NPE occurred
    verify(mockOpContext, atLeast(1)).getMetricUtils();

    // Verify HTTP client was called expected number of times
    verify(mockHttpClient, times(2)).execute(any(HttpUriRequest.class));
  }
}
