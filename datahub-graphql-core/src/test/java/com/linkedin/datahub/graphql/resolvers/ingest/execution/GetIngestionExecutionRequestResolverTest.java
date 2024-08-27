package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetIngestionExecutionRequestResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ExecutionRequestInput returnedInput = getTestExecutionRequestInput();
    ExecutionRequestResult returnedResult = getTestExecutionRequestResult();

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN))),
                Mockito.eq(
                    ImmutableSet.of(
                        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                        Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_EXECUTION_REQUEST_URN,
                new EntityResponse()
                    .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
                    .setUrn(TEST_EXECUTION_REQUEST_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(returnedInput.data()))
                                    .setCreated(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(
                                                Urn.createFromString("urn:li:corpuser:test"))),
                                Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(returnedResult.data()))
                                    .setCreated(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(
                                                Urn.createFromString("urn:li:corpuser:test"))))))));
    GetIngestionExecutionRequestResolver resolver =
        new GetIngestionExecutionRequestResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_EXECUTION_REQUEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    verifyTestExecutionRequest(resolver.get(mockEnv).get(), returnedInput, returnedResult);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GetIngestionExecutionRequestResolver resolver =
        new GetIngestionExecutionRequestResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_EXECUTION_REQUEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    GetIngestionExecutionRequestResolver resolver =
        new GetIngestionExecutionRequestResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_EXECUTION_REQUEST_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
