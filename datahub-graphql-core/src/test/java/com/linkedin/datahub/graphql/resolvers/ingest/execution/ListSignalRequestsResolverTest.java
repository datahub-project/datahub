package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListSignalRequestsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestSignal;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListSignalRequestsResolverTest {
  public static final String TEST_URN = "urn:li:executionRequest:test-urn";
  private static final List<String> URN_STRINGS = new ArrayList<>(Arrays.asList(TEST_URN));
  private static final ListSignalRequestsInput TEST_INPUT =
      new ListSignalRequestsInput(URN_STRINGS);
  private static final Set<Urn> URNS =
      URN_STRINGS.stream()
          .map(
              (urnStr) -> {
                try {
                  return Urn.createFromString(urnStr);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to convert urn", e);
                }
              })
          .collect(Collectors.toSet());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ExecutionRequestSignal returnedInfo = new ExecutionRequestSignal();
    returnedInfo.setExecutorId("default");
    returnedInfo.setSignal("KILL");

    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq(URNS),
                Mockito.eq(ImmutableSet.of(Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_URN),
                new EntityResponse()
                    .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(returnedInfo.data())))))));

    ListSignalRequestsResolver resolver = new ListSignalRequestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals(resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getSignalRequests().size(), 1);
    assertEquals(resolver.get(mockEnv).get().getSignalRequests().get(0).getExecId(), TEST_URN);
    assertEquals(resolver.get(mockEnv).get().getSignalRequests().get(0).getExecutorId(), "default");
    assertEquals(resolver.get(mockEnv).get().getSignalRequests().get(0).getSignal(), "KILL");
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(OperationContext.class), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    ListSignalRequestsResolver resolver = new ListSignalRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
