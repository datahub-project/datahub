package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteMonitorResolverTest {

  private static final String TEST_MONITOR_URN =
      "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD),test)";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);

    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(Urn.createFromString(TEST_MONITOR_URN)),
                anyBoolean()))
        .thenReturn(true);

    DeleteMonitorResolver resolver = new DeleteMonitorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_MONITOR_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), eq(Urn.createFromString(TEST_MONITOR_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(
            any(OperationContext.class), eq(Urn.createFromString(TEST_MONITOR_URN)), anyBoolean());
  }

  @Test
  public void testGetSuccessMonitorAlreadyRemoved() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(List.of(Urn.createFromString(TEST_MONITOR_URN))),
                anyBoolean()))
        .thenReturn(Set.of());

    DeleteMonitorResolver resolver = new DeleteMonitorResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_MONITOR_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(any(OperationContext.class), eq(Urn.createFromString(TEST_MONITOR_URN)));

    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntityReferences(
            any(OperationContext.class), eq(Urn.createFromString(TEST_MONITOR_URN)));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(Urn.createFromString(TEST_MONITOR_URN)),
                anyBoolean()))
        .thenReturn(true);
    DeleteMonitorResolver resolver = new DeleteMonitorResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_MONITOR_URN);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(any(OperationContext.class), Mockito.any());
    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntityReferences(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .deleteEntity(any(OperationContext.class), Mockito.any());

    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(Urn.createFromString(TEST_MONITOR_URN)),
                anyBoolean()))
        .thenReturn(true);

    DeleteMonitorResolver resolver = new DeleteMonitorResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(TEST_MONITOR_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
