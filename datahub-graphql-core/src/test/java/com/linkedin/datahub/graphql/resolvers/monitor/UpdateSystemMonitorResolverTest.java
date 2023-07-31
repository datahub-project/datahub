package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SystemMonitorType;
import com.linkedin.datahub.graphql.generated.UpdateSystemMonitorInput;
import com.linkedin.datahub.graphql.generated.UpdateSystemMonitorsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorMode;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class UpdateSystemMonitorResolverTest {

  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final Urn TEST_MONITOR_URN = UrnUtils.getUrn(
      String.format("urn:li:monitor:(%s,%s)", TEST_ENTITY_URN, AcrylConstants.FRESHNESS_SYSTEM_MONITOR_ID));

  private static final UpdateSystemMonitorsInput TEST_INPUT = new UpdateSystemMonitorsInput(
      TEST_ENTITY_URN.toString(),
      ImmutableList.of(
        new UpdateSystemMonitorInput(
            SystemMonitorType.FRESHNESS,
            com.linkedin.datahub.graphql.generated.MonitorMode.INACTIVE
        )
      )
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(true);
    UpdateSystemMonitorsResolver resolver = new UpdateSystemMonitorsResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Validate that we updated the monitor
    Mockito.verify(mockService, Mockito.times(1)).upsertMonitorMode(
        Mockito.eq(TEST_MONITOR_URN),
        Mockito.eq(MonitorMode.INACTIVE),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityDoesNotExist() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(false);
    UpdateSystemMonitorsResolver resolver = new UpdateSystemMonitorsResolver(mockService, mockClient);

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
    UpdateSystemMonitorsResolver resolver = new UpdateSystemMonitorsResolver(mockService, mockClient);

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
    Mockito.doThrow(RuntimeException.class).when(mockService).upsertMonitorMode(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class));

    UpdateSystemMonitorsResolver resolver = new UpdateSystemMonitorsResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockService() throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);

    Mockito.when(service.upsertMonitorMode(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class)
    )).thenReturn(TEST_MONITOR_URN);

    return service;
  }

  private EntityClient initMockClient(boolean exists) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    if (exists) {
      Mockito.when(client.exists(
          Mockito.eq(TEST_ENTITY_URN),
          Mockito.any(Authentication.class)
      )).thenReturn(true);
    } else {
      Mockito.when(client.exists(
          Mockito.eq(TEST_ENTITY_URN),
          Mockito.any(Authentication.class)
      )).thenReturn(false);
    }
    return client;
  }
}