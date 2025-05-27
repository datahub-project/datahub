package com.linkedin.datahub.graphql.types.ingestion;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ExecutionRequestTypeTest {

  private static final String TEST_1_URN = "urn:li:dataHubExecutionRequest:id-1";
  private static final String TEST_INPUT_TASK = "RUN_INGEST";
  private static final Long TEST_INPUT_REQUESTED_AT = 0L;
  private static final String TEST_RESULT_STATUS = "FAILED";
  private static final ExecutionRequestInput TEST_INPUT =
      new ExecutionRequestInput().setTask(TEST_INPUT_TASK).setRequestedAt(TEST_INPUT_REQUESTED_AT);
  private static final ExecutionRequestResult TEST_RESULT =
      new ExecutionRequestResult().setStatus(TEST_RESULT_STATUS);

  private static final String TEST_2_URN = "urn:li:dataHubExecutionRequest:id-2";

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn executionRequestUrn1 = Urn.createFromString(TEST_1_URN);
    Urn executionRequestUrn2 = Urn.createFromString(TEST_2_URN);
    Urn ingestionSourceUrn = Urn.createFromString("urn:li:dataHubIngestionSource:id-1");

    EntityResponse entityResponse =
        getEntityResponse()
            .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
            .setUrn(executionRequestUrn1);
    ExecutionRequestInput requestInput =
        getExecutionRequestInput(
            getExecutionRequestInputArts("recipe", "version"),
            "task",
            "executorId",
            0L,
            getExecutionRequestSource(ingestionSourceUrn, "type"));
    ExecutionRequestResult requestResult = getExecutionRequestResult(10L, "report", 0L, "success");
    addAspect(entityResponse, Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME, requestInput);
    addAspect(entityResponse, Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME, requestResult);

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq(
                    new HashSet<>(ImmutableSet.of(executionRequestUrn1, executionRequestUrn2))),
                Mockito.eq(ExecutionRequestType.ASPECTS_TO_FETCH)))
        .thenReturn(ImmutableMap.of(executionRequestUrn1, entityResponse));

    ExecutionRequestType type = new ExecutionRequestType(client);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<ExecutionRequest>> result =
        type.batchLoad(ImmutableList.of(TEST_1_URN, TEST_2_URN), mockContext);

    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(executionRequestUrn1, executionRequestUrn2)),
            Mockito.eq(ExecutionRequestType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    ExecutionRequest executionRequest1 = result.get(0).getData();
    assertEquals(executionRequest1.getUrn(), TEST_1_URN);
    assertEquals(executionRequest1.getType(), EntityType.EXECUTION_REQUEST);
    verifyExecutionRequestInput(executionRequest1, requestInput);
    verifyExecutionRequestResult(executionRequest1, requestResult);
    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    ExecutionRequestType type = new ExecutionRequestType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_1_URN, TEST_2_URN), context));
  }
}
