package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SystemMonitorType;
import com.linkedin.datahub.graphql.generated.SystemMonitorsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.service.MonitorService;
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


public class SystemMonitorsResolverTest {

  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final Urn TEST_MONITOR_URN = UrnUtils.getUrn(
      String.format("urn:li:monitor:(%s,%s)", TEST_ENTITY_URN.toString(), AcrylConstants.FRESHNESS_SYSTEM_MONITOR_ID));
  private static final MonitorKey TEST_MONITOR_KEY = new MonitorKey()
      .setEntity(UrnUtils.getUrn(TEST_MONITOR_URN.getEntityKey().get(0)))
      .setId(TEST_MONITOR_URN.getEntityKey().get(1)
      );
  private static final MonitorInfo TEST_MONITOR_INFO = new MonitorInfo()
      .setType(MonitorType.ASSERTION)
      .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(true);
    SystemMonitorsResolver resolver = new SystemMonitorsResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    SystemMonitorsResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.getMonitors().get(0).getType().toString(), SystemMonitorType.FRESHNESS.toString());
    assertEquals(result.getMonitors().get(0).getMonitor().getUrn(), TEST_MONITOR_URN.toString());
  }

  @Test
  public void testGetEntityDoesNotExist() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(false);
    SystemMonitorsResolver resolver = new SystemMonitorsResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetMonitorServiceException() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService();
    EntityClient mockClient = initMockClient(true);
    Mockito.doThrow(RuntimeException.class).when(mockService).getMonitorInfo(Mockito.any());

    SystemMonitorsResolver resolver = new SystemMonitorsResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ENTITY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockService() throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);

    Mockito.when(service.getMonitorInfo(
        Mockito.eq(TEST_MONITOR_URN)
    )).thenReturn(TEST_MONITOR_INFO);

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