package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.Authentication;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStatus;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorError;
import com.linkedin.monitor.MonitorErrorType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorStatus;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionStatusResolverTest {

  @Test
  public void testUsesRunSummaryAssertionStatus() throws Exception {
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionStatusResolver resolver =
        new AssertionStatusResolver(assertionService, monitorService);

    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:status-1");
    AssertionRunSummary summary = new AssertionRunSummary();
    summary.setAssertionStatus(com.linkedin.assertion.AssertionStatus.PASSING);
    Mockito.when(assertionService.getAssertionRunSummary(any(), eq(assertionUrn)))
        .thenReturn(summary);

    AssertionStatus status = resolver.get(mockEnv(assertionUrn.toString())).get();
    assertEquals(status, AssertionStatus.PASSING);
  }

  @Test
  public void testComputesStatusFromMonitorAndRunEvent() throws Exception {
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionStatusResolver resolver =
        new AssertionStatusResolver(assertionService, monitorService);

    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:status-2");
    Mockito.when(assertionService.getAssertionRunSummary(any(), eq(assertionUrn))).thenReturn(null);

    Urn monitorUrn = UrnUtils.getUrn("urn:li:monitor:status-2");
    Mockito.when(assertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(monitorUrn);

    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setError(new MonitorError().setType(MonitorErrorType.INPUT_DATA_INVALID));
    MonitorInfo monitorInfo = new MonitorInfo().setStatus(monitorStatus);
    Mockito.when(monitorService.getMonitorInfo(any(), eq(monitorUrn))).thenReturn(monitorInfo);

    AssertionRunEvent runEvent =
        new AssertionRunEvent()
            .setResult(new AssertionResult().setType(AssertionResultType.SUCCESS));
    Mockito.when(assertionService.getLatestAssertionRunEvent(any(), eq(assertionUrn)))
        .thenReturn(runEvent);

    AssertionStatus status = resolver.get(mockEnv(assertionUrn.toString())).get();
    assertEquals(status, AssertionStatus.ERROR);
  }

  @Test
  public void testReturnsNullWhenNoRunSummaryMonitorOrRunEvent() throws Exception {
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionStatusResolver resolver =
        new AssertionStatusResolver(assertionService, monitorService);

    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:status-3");
    Mockito.when(assertionService.getAssertionRunSummary(any(), eq(assertionUrn))).thenReturn(null);
    Mockito.when(assertionService.getMonitorUrnForAssertion(any(), eq(assertionUrn)))
        .thenReturn(null);
    Mockito.when(assertionService.getLatestAssertionRunEvent(any(), eq(assertionUrn)))
        .thenReturn(null);

    AssertionStatus status = resolver.get(mockEnv(assertionUrn.toString())).get();
    assertNull(status);
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
