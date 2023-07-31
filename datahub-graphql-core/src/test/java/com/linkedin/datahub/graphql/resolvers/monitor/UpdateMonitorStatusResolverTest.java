package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.UpdateMonitorStatusInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorType;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class UpdateMonitorStatusResolverTest {

  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_MONITOR_URN = UrnUtils.getUrn(
      String.format("urn:li:monitor:(%s,%s)", TEST_ENTITY_URN.toString(), AcrylConstants.FRESHNESS_SYSTEM_MONITOR_ID));

  private static final UpdateMonitorStatusInput TEST_INPUT = new UpdateMonitorStatusInput(
      TEST_MONITOR_URN.toString(),
      com.linkedin.datahub.graphql.generated.MonitorMode.INACTIVE
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(MonitorMode.valueOf(TEST_INPUT.getMode().toString()));
    EntityClient mockClient = initMockClient(true);
    UpdateMonitorStatusResolver resolver = new UpdateMonitorStatusResolver(mockService, mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor result = resolver.get(mockEnv).get();

    assertEquals(result.getInfo().getStatus().getMode(), TEST_INPUT.getMode());

    // Validate that we updated the monitor
    Mockito.verify(mockService, Mockito.times(1)).upsertMonitorMode(
        Mockito.eq(TEST_MONITOR_URN),
        Mockito.eq(MonitorMode.INACTIVE),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityDoesNotExist() throws Exception {
    // Create resolver
    MonitorService mockService = initMockService(MonitorMode.INACTIVE);
    EntityClient mockClient = initMockClient(false);
    UpdateMonitorStatusResolver resolver = new UpdateMonitorStatusResolver(mockService, mockClient);

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
    MonitorService mockService = initMockService(MonitorMode.INACTIVE);
    UpdateMonitorStatusResolver resolver = new UpdateMonitorStatusResolver(mockService, mockClient);

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
    MonitorService mockService = initMockService(MonitorMode.INACTIVE);
    EntityClient mockClient = initMockClient(true);
    Mockito.doThrow(RuntimeException.class).when(mockService).upsertMonitorMode(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class));

    UpdateMonitorStatusResolver resolver = new UpdateMonitorStatusResolver(mockService, mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private MonitorService initMockService(MonitorMode expectedMode) throws Exception {
    MonitorService service = Mockito.mock(MonitorService.class);

    Mockito.when(service.upsertMonitorMode(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class)
    )).thenReturn(TEST_MONITOR_URN);

    MonitorInfo info = new MonitorInfo()
        .setType(MonitorType.ASSERTION)
        .setStatus(new com.linkedin.monitor.MonitorStatus()
          .setMode(expectedMode)
        );

    Mockito.when(service.getMonitorEntityResponse(
        Mockito.eq(TEST_MONITOR_URN),
        Mockito.any(Authentication.class)
    )).thenReturn(new EntityResponse()
      .setUrn(TEST_MONITOR_URN)
      .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
          Constants.MONITOR_INFO_ASPECT_NAME,
          new EnvelopedAspect()
            .setName(Constants.MONITOR_INFO_ASPECT_NAME)
            .setVersion(0L)
            .setValue(new Aspect(info.data()))
      )))
    );

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