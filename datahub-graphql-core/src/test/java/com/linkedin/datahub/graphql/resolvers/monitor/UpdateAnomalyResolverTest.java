package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateAnomalyInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateAnomalyResolverTest {

  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,testMonitor)", TEST_ENTITY_URN.toString()));
  private static final long TEST_ANOMALY_TIMESTAMP = 1630000000000L;

  private static final UpdateAnomalyInput TEST_INPUT =
      new UpdateAnomalyInput(
          TEST_MONITOR_URN.toString(),
          TEST_ANOMALY_TIMESTAMP,
          com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(AnomalyReviewState.CONFIRMED);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    UpdateAnomalyResolver resolver = new UpdateAnomalyResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent result = resolver.get(mockEnv).get();

    assertEquals(result.getState(), TEST_INPUT.getState());
    assertEquals(result.getTimestampMillis(), TEST_INPUT.getAnomalyTimestampMillis());

    // Verify ingestProposal was called with correct params
    Mockito.verify(mockClient, Mockito.times(1))
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
  public void testGetAnomalyNotFound() throws Exception {
    // Create resolver
    MonitorService mockService = Mockito.mock(MonitorService.class);
    Mockito.when(
            mockService.getAnomalyEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(TEST_ANOMALY_TIMESTAMP)))
        .thenReturn(null);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    UpdateAnomalyResolver resolver = new UpdateAnomalyResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetMonitorServiceException() throws Exception {
    // Create resolver
    MonitorService mockService = Mockito.mock(MonitorService.class);
    Mockito.when(
            mockService.getAnomalyEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(TEST_ANOMALY_TIMESTAMP)))
        .thenThrow(new RuntimeException("Service error"));

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    UpdateAnomalyResolver resolver = new UpdateAnomalyResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetIngestProposalException() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(AnomalyReviewState.CONFIRMED);
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RuntimeException("Ingest error"))
        .when(mockClient)
        .ingestProposal(any(OperationContext.class), Mockito.any());

    UpdateAnomalyResolver resolver = new UpdateAnomalyResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockService(AnomalyReviewState expectedState) throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);

    AnomalySource source = new AnomalySource();
    source.setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE);
    source.setSourceUrn(TEST_ENTITY_URN);
    source.setProperties(new AnomalySourceProperties());

    MonitorAnomalyEvent event = new MonitorAnomalyEvent();
    event.setTimestampMillis(TEST_ANOMALY_TIMESTAMP);
    event.setSource(source);
    event.setCreated(new TimeStamp().setTime(System.currentTimeMillis()));
    event.setLastUpdated(new TimeStamp().setTime(System.currentTimeMillis()));
    event.setState(expectedState);

    Mockito.when(
            service.getAnomalyEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(TEST_ANOMALY_TIMESTAMP)))
        .thenReturn(event);

    return service;
  }
}
