package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionHealth;
import com.linkedin.datahub.graphql.generated.AssertionHealthStatus;
import com.linkedin.datahub.graphql.generated.MonitorErrorType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.monitor.MonitorError;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionHealthResolverTest {

  @Test
  public void testNoMonitorNoRunEventsReturnsHealthy() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(mockClient, mockAssertionService);

    final Urn assertionUrn = Urn.createFromString("urn:li:assertion:health-1");
    Mockito.when(mockAssertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(null);
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(),
                eq(assertionUrn.toString()),
                eq(Constants.ASSERTION_ENTITY_NAME),
                eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                eq(1),
                any()))
        .thenReturn(Collections.emptyList());

    AssertionHealth health = resolver.get(mockEnv(assertionUrn.toString())).get();

    assertEquals(health.getStatus(), AssertionHealthStatus.HEALTHY);
    assertNull(health.getMonitorError());
    assertNull(health.getEvaluationError());
  }

  @Test
  public void testMonitorTrainingDataInsufficientIsHealthy() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(mockClient, mockAssertionService);

    final Urn assertionUrn = Urn.createFromString("urn:li:assertion:health-2");
    final Urn monitorUrn = Urn.createFromString("urn:li:monitor:health-2");

    Mockito.when(mockAssertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(monitorUrn);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.MONITOR_ENTITY_NAME),
                eq(monitorUrn),
                eq(Set.of(Constants.MONITOR_KEY_ASPECT_NAME, Constants.MONITOR_INFO_ASPECT_NAME)),
                eq(false)))
        .thenReturn(
            buildMonitorResponse(
                monitorUrn, com.linkedin.monitor.MonitorErrorType.TRAINING_DATA_INSUFFICIENT));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(),
                eq(assertionUrn.toString()),
                eq(Constants.ASSERTION_ENTITY_NAME),
                eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                eq(1),
                any()))
        .thenReturn(Collections.emptyList());

    AssertionHealth health = resolver.get(mockEnv(assertionUrn.toString())).get();

    assertEquals(health.getStatus(), AssertionHealthStatus.HEALTHY);
    assertNotNull(health.getMonitorError());
    assertEquals(health.getMonitorError().getType(), MonitorErrorType.TRAINING_DATA_INSUFFICIENT);
    assertNotNull(health.getRecommendedAction());
    assertNotNull(health.getDisplayMessage());
  }

  @Test
  public void testEvaluationStatePersistenceFailureIsDegraded() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(AssertionResultErrorType.STATE_PERSISTENCE_FAILED);

    assertEquals(health.getStatus(), AssertionHealthStatus.DEGRADED);
    assertNotNull(health.getEvaluationError());
    assertEquals(
        health.getEvaluationError().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultErrorType.STATE_PERSISTENCE_FAILED);
  }

  @Test
  public void testEvaluationInvalidParametersIsError() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(AssertionResultErrorType.INVALID_PARAMETERS);

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
    assertNotNull(health.getEvaluationError());
    assertEquals(
        health.getEvaluationError().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultErrorType.INVALID_PARAMETERS);
  }

  @Test
  public void testEvaluationSourceQueryFailedBadRequestIsError() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(
            AssertionResultErrorType.SOURCE_QUERY_FAILED, "BadRequest: 400 invalid query");

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
    assertNotNull(health.getDisplayMessage());
  }

  @Test
  public void testEvaluationSourceQueryFailedTimeoutIsDegraded() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(
            AssertionResultErrorType.SOURCE_QUERY_FAILED, "ServiceUnavailable: 503 timeout");

    assertEquals(health.getStatus(), AssertionHealthStatus.DEGRADED);
  }

  @Test
  public void testEvaluationSourceQueryFailedResponseCodeIsError() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(AssertionResultErrorType.SOURCE_QUERY_FAILED, "message", "403");

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
  }

  @Test
  public void testEvaluationSourceQueryFailedSnowflakePrivilegeIsError() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(
            AssertionResultErrorType.SOURCE_QUERY_FAILED,
            "Source query (Snowflake) failed with error: 003001 (42501): SQL access control error: Insufficient privileges");

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
  }

  @Test
  public void testEvaluationSourceQueryFailedSqlStateIsError() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(
            AssertionResultErrorType.SOURCE_QUERY_FAILED, null, null, "42501");

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
  }

  @Test
  public void testEvaluationSourceQueryFailedSqlStateTransientIsDegraded() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(
            AssertionResultErrorType.SOURCE_QUERY_FAILED, null, null, "08006");

    assertEquals(health.getStatus(), AssertionHealthStatus.DEGRADED);
  }

  @Test
  public void testEvaluationSourceQueryFailedResponseCodeTakesPrecedence() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(
            AssertionResultErrorType.SOURCE_QUERY_FAILED, null, "403", "08006");

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
  }

  @Test
  public void testEvaluationSourceQueryFailedResponseCodeServerErrorIsDegraded() throws Exception {
    AssertionHealth health =
        resolveWithEvaluationError(AssertionResultErrorType.SOURCE_QUERY_FAILED, "message", "503");

    assertEquals(health.getStatus(), AssertionHealthStatus.DEGRADED);
  }

  @Test
  public void testEvaluationErrorWithNullTypeIsUnknown() throws Exception {
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(
            Mockito.mock(EntityClient.class), Mockito.mock(AssertionService.class));
    Method method =
        AssertionHealthResolver.class.getDeclaredMethod(
            "mapEvaluationErrorToStatus",
            com.linkedin.datahub.graphql.generated.AssertionResultError.class);
    method.setAccessible(true);
    com.linkedin.datahub.graphql.generated.AssertionResultError error =
        new com.linkedin.datahub.graphql.generated.AssertionResultError();
    AssertionHealthStatus status = (AssertionHealthStatus) method.invoke(resolver, error);

    assertEquals(status, AssertionHealthStatus.UNKNOWN);
  }

  @Test
  public void testMonitorPresentWithoutErrorIsHealthy() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(mockClient, mockAssertionService);

    final Urn assertionUrn = Urn.createFromString("urn:li:assertion:health-monitor");
    final Urn monitorUrn = Urn.createFromString("urn:li:monitor:health-monitor");

    Mockito.when(mockAssertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(monitorUrn);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.MONITOR_ENTITY_NAME),
                eq(monitorUrn),
                eq(Set.of(Constants.MONITOR_KEY_ASPECT_NAME, Constants.MONITOR_INFO_ASPECT_NAME)),
                eq(false)))
        .thenReturn(buildMonitorResponseWithNullError(monitorUrn));
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(),
                eq(assertionUrn.toString()),
                eq(Constants.ASSERTION_ENTITY_NAME),
                eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                eq(1),
                any()))
        .thenReturn(Collections.emptyList());

    AssertionHealth health = resolver.get(mockEnv(assertionUrn.toString())).get();

    assertEquals(health.getStatus(), AssertionHealthStatus.HEALTHY);
    assertNull(health.getEvaluationError());
  }

  @Test
  public void testEvaluationErrorWinsOverMonitorDegraded() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(mockClient, mockAssertionService);

    final Urn assertionUrn = Urn.createFromString("urn:li:assertion:health-override");
    final Urn monitorUrn = Urn.createFromString("urn:li:monitor:health-override");

    Mockito.when(mockAssertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(monitorUrn);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.MONITOR_ENTITY_NAME),
                eq(monitorUrn),
                eq(Set.of(Constants.MONITOR_KEY_ASPECT_NAME, Constants.MONITOR_INFO_ASPECT_NAME)),
                eq(false)))
        .thenReturn(
            buildMonitorResponse(
                monitorUrn, com.linkedin.monitor.MonitorErrorType.INPUT_DATA_INVALID));
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(),
                eq(assertionUrn.toString()),
                eq(Constants.ASSERTION_ENTITY_NAME),
                eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                eq(1),
                any()))
        .thenReturn(
            ImmutableList.of(
                new com.linkedin.metadata.aspect.EnvelopedAspect()
                    .setAspect(
                        GenericRecordUtils.serializeAspect(
                            buildRunEventWithError(
                                assertionUrn,
                                AssertionResultErrorType.INVALID_PARAMETERS,
                                null,
                                null,
                                null)))));

    AssertionHealth health = resolver.get(mockEnv(assertionUrn.toString())).get();

    assertEquals(health.getStatus(), AssertionHealthStatus.ERROR);
  }

  @Test
  public void testAllMonitorErrorTypesAreHandled() throws Exception {
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(
            Mockito.mock(EntityClient.class), Mockito.mock(AssertionService.class));
    Method method =
        AssertionHealthResolver.class.getDeclaredMethod(
            "mapMonitorErrorToStatus", com.linkedin.datahub.graphql.generated.MonitorError.class);
    method.setAccessible(true);
    for (MonitorErrorType type : MonitorErrorType.values()) {
      com.linkedin.datahub.graphql.generated.MonitorError error =
          new com.linkedin.datahub.graphql.generated.MonitorError();
      error.setType(type);
      AssertionHealthStatus status = (AssertionHealthStatus) method.invoke(resolver, error);
      assertNotNull(status);
    }
  }

  @Test
  public void testAllEvaluationErrorTypesAreHandled() throws Exception {
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(
            Mockito.mock(EntityClient.class), Mockito.mock(AssertionService.class));
    Method method =
        AssertionHealthResolver.class.getDeclaredMethod(
            "mapEvaluationErrorToStatus",
            com.linkedin.datahub.graphql.generated.AssertionResultError.class);
    method.setAccessible(true);
    for (com.linkedin.datahub.graphql.generated.AssertionResultErrorType type :
        com.linkedin.datahub.graphql.generated.AssertionResultErrorType.values()) {
      com.linkedin.datahub.graphql.generated.AssertionResultError error =
          new com.linkedin.datahub.graphql.generated.AssertionResultError();
      error.setType(type);
      AssertionHealthStatus status = (AssertionHealthStatus) method.invoke(resolver, error);
      assertNotNull(status);
    }
  }

  private AssertionHealth resolveWithEvaluationError(AssertionResultErrorType errorType)
      throws Exception {
    return resolveWithEvaluationError(errorType, null);
  }

  private AssertionHealth resolveWithEvaluationError(
      AssertionResultErrorType errorType, String message) throws Exception {
    return resolveWithEvaluationError(errorType, message, null);
  }

  private AssertionHealth resolveWithEvaluationError(
      AssertionResultErrorType errorType, String message, String responseCode) throws Exception {
    return resolveWithEvaluationError(errorType, message, responseCode, null);
  }

  private AssertionHealth resolveWithEvaluationError(
      AssertionResultErrorType errorType, String message, String responseCode, String sqlState)
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    AssertionHealthResolver resolver =
        new AssertionHealthResolver(mockClient, mockAssertionService);

    final Urn assertionUrn = Urn.createFromString("urn:li:assertion:health-3");

    AssertionRunEvent runEvent =
        buildRunEventWithError(assertionUrn, errorType, message, responseCode, sqlState);

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(),
                eq(assertionUrn.toString()),
                eq(Constants.ASSERTION_ENTITY_NAME),
                eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                eq(1),
                any()))
        .thenReturn(
            ImmutableList.of(
                new com.linkedin.metadata.aspect.EnvelopedAspect()
                    .setAspect(GenericRecordUtils.serializeAspect(runEvent))));

    Mockito.when(mockAssertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(null);

    return resolver.get(mockEnv(assertionUrn.toString())).get();
  }

  private AssertionRunEvent buildRunEventWithError(
      Urn assertionUrn,
      AssertionResultErrorType errorType,
      String message,
      String responseCode,
      String sqlState) {
    AssertionResultError error = new AssertionResultError();
    if (errorType != null) {
      error.setType(errorType);
    }
    if (message != null || responseCode != null || sqlState != null) {
      StringMap properties = new StringMap();
      if (message != null) {
        properties.put("message", message);
      }
      if (responseCode != null) {
        properties.put("response_code", responseCode);
      }
      if (sqlState != null) {
        properties.put("sqlstate", sqlState);
      }
      error.setProperties(properties);
    }
    return new AssertionRunEvent()
        .setAssertionUrn(assertionUrn)
        .setAsserteeUrn(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))
        .setRunId("run-id")
        .setStatus(AssertionRunStatus.COMPLETE)
        .setTimestampMillis(42L)
        .setResult(new AssertionResult().setType(AssertionResultType.ERROR).setError(error));
  }

  private EntityResponse buildMonitorResponseWithNullError(Urn monitorUrn) {
    MonitorStatus status = new MonitorStatus().setMode(MonitorMode.ACTIVE);
    MonitorInfo info = new MonitorInfo().setType(MonitorType.ASSERTION).setStatus(status);
    MonitorKey key =
        new MonitorKey()
            .setEntity(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))
            .setId("health-test");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.MONITOR_KEY_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(key.data())));
    aspects.put(
        Constants.MONITOR_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(info.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(monitorUrn);
    response.setEntityName(Constants.MONITOR_ENTITY_NAME);
    response.setAspects(aspects);
    return response;
  }

  private EntityResponse buildMonitorResponse(
      Urn monitorUrn, com.linkedin.monitor.MonitorErrorType errorType) {
    MonitorStatus status = new MonitorStatus().setMode(MonitorMode.ACTIVE);
    status.setError(new MonitorError().setType(errorType));

    MonitorInfo info = new MonitorInfo().setType(MonitorType.ASSERTION).setStatus(status);
    MonitorKey key =
        new MonitorKey()
            .setEntity(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))
            .setId("health-test");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.MONITOR_KEY_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(key.data())));
    aspects.put(
        Constants.MONITOR_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(info.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(monitorUrn);
    response.setEntityName(Constants.MONITOR_ENTITY_NAME);
    response.setAspects(aspects);
    return response;
  }

  private DataFetchingEnvironment mockEnv(String assertionUrn) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    OperationContext operationContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Assertion assertion = new Assertion();
    assertion.setUrn(assertionUrn);
    Mockito.when(mockEnv.getSource()).thenReturn(assertion);
    return mockEnv;
  }
}
