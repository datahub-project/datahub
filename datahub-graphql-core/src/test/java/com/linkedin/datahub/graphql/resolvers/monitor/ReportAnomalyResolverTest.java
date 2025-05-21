package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ReportAnomalyInput;
import com.linkedin.datahub.graphql.resolvers.assertion.ReportAnomalyResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import java.util.function.LongSupplier;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ReportAnomalyResolverTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:1234");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,testMonitor)", TEST_ENTITY_URN.toString()));
  private static final long TEST_RUN_EVENT_TIMESTAMP = 1640000000000L;
  private static final long FIXED_TIME = 1650000000000L;

  private static final ReportAnomalyInput TEST_INPUT =
      new ReportAnomalyInput(
          TEST_MONITOR_URN.toString(), TEST_ASSERTION_URN.toString(), TEST_RUN_EVENT_TIMESTAMP);

  @Test
  public void testGetSuccess() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService(null); // No existing anomaly
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    LongSupplier fixedTimeProvider = () -> FIXED_TIME;

    // Create resolver
    ReportAnomalyResolver resolver =
        new ReportAnomalyResolver(
            mockAssertionService, mockMonitorService, mockEntityClient, fixedTimeProvider);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent result = resolver.get(mockEnv).get();

    // Verify result
    assertEquals(
        result.getState(), com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
    assertEquals(result.getTimestampMillis(), TEST_RUN_EVENT_TIMESTAMP);

    // Verify ingestProposal was called with correct params
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                proposal ->
                    proposal.getEntityUrn().equals(TEST_MONITOR_URN)
                        && proposal
                            .getAspectName()
                            .equals(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME)));
  }

  @Test
  public void testGetWithExistingAnomalyEvent() throws Exception {
    // Create mocks with existing anomaly event
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    MonitorService mockMonitorService = initMockMonitorService(createMockAnomalyEvent());
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Create resolver
    ReportAnomalyResolver resolver =
        new ReportAnomalyResolver(mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw an exception due to existing anomaly
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetWithMissingRunEvent() throws Exception {
    // Create mocks with null run event
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    Mockito.when(
            mockAssertionService.getAssertionRunEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_RUN_EVENT_TIMESTAMP)))
        .thenReturn(null);

    MonitorService mockMonitorService = initMockMonitorService(null);
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Create resolver
    ReportAnomalyResolver resolver =
        new ReportAnomalyResolver(mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw an exception due to missing run event
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetWithIngestError() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService(null);
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Configure entity client to throw exception
    Mockito.doThrow(new RemoteInvocationException("Ingest error"))
        .when(mockEntityClient)
        .ingestProposal(any(OperationContext.class), Mockito.any());

    // Create resolver
    ReportAnomalyResolver resolver =
        new ReportAnomalyResolver(mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw exception due to ingest error
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private AssertionService initMockAssertionService() {
    AssertionService service = Mockito.mock(AssertionService.class);

    // Create a mock run event
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(TEST_RUN_EVENT_TIMESTAMP);
    runEvent.setResult(new AssertionResult());

    Mockito.when(
            service.getAssertionRunEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_RUN_EVENT_TIMESTAMP)))
        .thenReturn(runEvent);

    return service;
  }

  private MonitorService initMockMonitorService(MonitorAnomalyEvent existingEvent) {
    MonitorService service = Mockito.mock(MonitorService.class);

    Mockito.when(
            service.getAnomalyEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(TEST_RUN_EVENT_TIMESTAMP)))
        .thenReturn(existingEvent);

    return service;
  }

  private MonitorAnomalyEvent createMockAnomalyEvent() {
    AnomalySource source = new AnomalySource();
    source.setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE);
    source.setSourceUrn(TEST_ENTITY_URN);
    source.setProperties(new AnomalySourceProperties());

    MonitorAnomalyEvent event = new MonitorAnomalyEvent();
    event.setTimestampMillis(TEST_RUN_EVENT_TIMESTAMP);
    event.setSource(source);
    event.setCreated(new TimeStamp().setTime(System.currentTimeMillis()));
    event.setLastUpdated(new TimeStamp().setTime(System.currentTimeMillis()));
    event.setState(AnomalyReviewState.REJECTED);

    return event;
  }
}
