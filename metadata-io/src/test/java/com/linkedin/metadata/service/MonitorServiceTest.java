package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
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
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFieldAssertionSourceType;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.schema.SchemaFieldSpec;
import com.linkedin.timeseries.CalendarInterval;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
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
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final Urn TEST_CONNECTION_URN = UrnUtils.getUrn("urn:li:dataPlatform:test");
  private static final String TEST_SQL_STATEMENT =
      "SELECT COUNT(*) FROM test_db.public.test_table;";
  private static final String TEST_HOST = "localhost";
  private static final Integer TEST_PORT = 9004;

  @Mock private BackoffPolicy backoffPolicy;

  @Test
  private void testGetMonitorInfo() throws Exception {
    final EntityClient mockClient = createMockEntityClient();
    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(Authentication.class));
    // Case 1: Info exists
    MonitorInfo info = service.getMonitorInfo(TEST_MONITOR_URN);
    Assert.assertEquals(info, mockMonitorInfo());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            Mockito.eq(Constants.MONITOR_ENTITY_NAME),
            Mockito.eq(TEST_MONITOR_URN),
            Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME)),
            Mockito.any(Authentication.class));

    // Case 2: Info does not exist
    info = service.getMonitorInfo(TEST_NON_EXISTENT_MONITOR_URN);
    Assert.assertNull(info);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            Mockito.eq(Constants.MONITOR_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_MONITOR_URN),
            Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME)),
            Mockito.any(Authentication.class));
  }

  @Test
  public void testCreateAssertionMonitorRequiredFields() throws Exception {
    // Test data and mocks
    EntityClient mockClient = Mockito.mock(EntityClient.class);
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

    Mockito.when(mockClient.exists(Mockito.eq(assertionUrn), Mockito.any(Authentication.class)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(Mockito.eq(entityUrn), Mockito.any(Authentication.class)))
        .thenReturn(true);

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(0);
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
        .batchIngestProposals(
            Mockito.anyList(), Mockito.any(Authentication.class), Mockito.eq(false));

    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(Authentication.class));

    // Test method
    Urn result =
        service.createAssertionMonitor(
            entityUrn,
            assertionUrn,
            schedule,
            parameters,
            null,
            Mockito.mock(Authentication.class));

    // Assert result
    Assert.assertEquals(result.getEntityType(), "monitor");
    Assert.assertEquals(result.getEntityKey().get(0), TEST_ENTITY_URN.toString());
  }

  @Test
  public void testCreateAssertionMonitorRequiredFieldsAssertionDoesNotExist() throws Exception {
    // Test data and mocks
    EntityClient mockClient = Mockito.mock(EntityClient.class);
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
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(Authentication.class));

    // Method should throw because the assertion does not exist.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.createAssertionMonitor(
                entityUrn,
                assertionUrn,
                schedule,
                parameters,
                null,
                Mockito.mock(Authentication.class)));
  }

  @Test
  public void testCreateAssertionMonitorRequiredFieldsEntityDoesNotExist() throws Exception {
    // Test data and mocks
    EntityClient mockClient = Mockito.mock(EntityClient.class);
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
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(Authentication.class));

    Mockito.when(mockClient.exists(Mockito.eq(assertionUrn), Mockito.any(Authentication.class)))
        .thenReturn(true);
    Mockito.when(mockClient.exists(Mockito.eq(entityUrn), Mockito.any(Authentication.class)))
        .thenReturn(false);

    // Method should throw because the assertion does not exist.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.createAssertionMonitor(
                entityUrn,
                assertionUrn,
                schedule,
                parameters,
                null,
                Mockito.mock(Authentication.class)));
  }

  @Test
  private void testUpsertMonitorMode() throws Exception {
    final EntityClient mockClient = createMockEntityClient();
    final MonitorService service =
        new MonitorService(
            TEST_HOST, TEST_PORT, false, mockClient, Mockito.mock(Authentication.class));

    // Case 1: Info exists
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(0);
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
        .batchIngestProposals(
            Mockito.anyList(), Mockito.any(Authentication.class), Mockito.eq(false));
    Urn urn =
        service.upsertMonitorMode(
            TEST_MONITOR_URN, MonitorMode.ACTIVE, Mockito.mock(Authentication.class));
    Assert.assertEquals(urn, TEST_MONITOR_URN);

    // Case 2: Info does not exist
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(0);
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
        .batchIngestProposals(
            Mockito.anyList(), Mockito.any(Authentication.class), Mockito.eq(false));
    urn =
        service.upsertMonitorMode(
            TEST_NON_EXISTENT_MONITOR_URN,
            MonitorMode.INACTIVE,
            Mockito.mock(Authentication.class));
    Assert.assertEquals(urn, TEST_NON_EXISTENT_MONITOR_URN);
  }

  private static EntityClient createMockEntityClient() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Init for monitor info
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
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
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_NON_EXISTENT_MONITOR_URN),
                Mockito.eq(ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_NON_EXISTENT_MONITOR_URN)
                .setEntityName(MONITOR_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

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

  @Test
  public void testFreshnessAssertionSuccess() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            Mockito.mock(Authentication.class),
            httpClient,
            backoffPolicy,
            3);

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
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            Mockito.mock(Authentication.class),
            httpClient,
            backoffPolicy,
            3);

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
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    final MonitorService service =
        new MonitorService(
            TEST_HOST,
            TEST_PORT,
            false,
            mockClient,
            Mockito.mock(Authentication.class),
            httpClient,
            backoffPolicy,
            3);

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
}
