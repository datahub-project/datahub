package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListExecutionRequestsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListExecutionRequestsResolverTest {

  private static final String TEST_EXECUTION_REQUEST_URN = "urn:li:executionRequest:test-execution";

  private static final ListExecutionRequestsInput TEST_INPUT =
      new ListExecutionRequestsInput(0, 20, null, null, null);

  private ExecutionRequestInput getTestExecutionRequestInput() {
    ExecutionRequestInput input = new ExecutionRequestInput();
    input.setTask("test-task");
    input.setRequestedAt(System.currentTimeMillis());
    return input;
  }

  private ExecutionRequestResult getTestExecutionRequestResult() {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("COMPLETED");
    result.setStartTimeMs(System.currentTimeMillis());
    result.setDurationMs(1000L);
    return result;
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ExecutionRequestInput returnedInput = getTestExecutionRequestInput();
    ExecutionRequestResult returnedResult = getTestExecutionRequestResult();

    // Mock search response
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity()
                                .setEntity(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))))));

    // Mock batch get response
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq(
                    new HashSet<>(
                        ImmutableSet.of(Urn.createFromString(TEST_EXECUTION_REQUEST_URN)))),
                Mockito.eq(
                    ImmutableSet.of(
                        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                        Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_EXECUTION_REQUEST_URN),
                new EntityResponse()
                    .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(returnedInput.data())),
                                Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(returnedResult.data())))))));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    var result = resolver.get(mockEnv).get();
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getExecutionRequests().size(), 1);

    var executionRequest = result.getExecutionRequests().get(0);
    assertEquals(executionRequest.getUrn(), TEST_EXECUTION_REQUEST_URN);
    assertEquals(executionRequest.getInput().getTask(), returnedInput.getTask());
    assertEquals(executionRequest.getResult().getStatus(), returnedResult.getStatus());
    assertEquals(executionRequest.getResult().getDurationMs(), returnedResult.getDurationMs());
  }

  @Test
  public void testGetWithNoResult() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ExecutionRequestInput returnedInput = getTestExecutionRequestInput();

    // Mock search and batch get with only input aspect
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity()
                                .setEntity(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq(
                    new HashSet<>(
                        ImmutableSet.of(Urn.createFromString(TEST_EXECUTION_REQUEST_URN)))),
                Mockito.eq(
                    ImmutableSet.of(
                        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                        Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_EXECUTION_REQUEST_URN),
                new EntityResponse()
                    .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(returnedInput.data())))))));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Verify execution request without result
    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 1);
    var executionRequest = result.getExecutionRequests().get(0);
    assertEquals(executionRequest.getInput().getTask(), returnedInput.getTask());
    assertNull(executionRequest.getResult());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetWithCustomQuery() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ListExecutionRequestsInput customInput =
        new ListExecutionRequestsInput(0, 20, "custom-query", null, null);

    // Verify custom query is passed to search
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("custom-query"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(customInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }
}
