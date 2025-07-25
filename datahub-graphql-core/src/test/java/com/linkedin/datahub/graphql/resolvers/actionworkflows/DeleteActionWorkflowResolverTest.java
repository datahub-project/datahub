package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteActionWorkflowResolverTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test";
  private static final String TEST_WORKFLOW_URN = "urn:li:actionWorkflow:123";

  private ActionWorkflowService mockActionWorkflowService;
  private DataFetchingEnvironment mockDataFetchingEnvironment;
  private DeleteActionWorkflowResolver resolver;

  @BeforeMethod
  public void setup() {
    mockActionWorkflowService = Mockito.mock(ActionWorkflowService.class);
    mockDataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    resolver = new DeleteActionWorkflowResolver(mockActionWorkflowService);
  }

  @Test
  public void testDeleteWorkflowSuccess() throws Exception {
    // GIVEN
    DeleteActionWorkflowInput input = new DeleteActionWorkflowInput();
    input.setUrn(TEST_WORKFLOW_URN);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN);
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);

    // Mock the service method - it should not throw any exception for successful deletion
    doNothing()
        .when(mockActionWorkflowService)
        .deleteActionWorkflow(any(OperationContext.class), any(Urn.class));

    // WHEN
    CompletableFuture<Boolean> futureResult = resolver.get(mockDataFetchingEnvironment);
    Boolean result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertTrue(result);
    verify(mockActionWorkflowService, times(1))
        .deleteActionWorkflow(any(OperationContext.class), eq(UrnUtils.getUrn(TEST_WORKFLOW_URN)));
  }

  @Test(expectedExceptions = java.util.concurrent.ExecutionException.class)
  public void testDeleteWorkflowAuthorizationFailure() throws Exception {
    // GIVEN
    DeleteActionWorkflowInput input = new DeleteActionWorkflowInput();
    input.setUrn(TEST_WORKFLOW_URN);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockDenyContext(TEST_USER_URN);
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);

    // WHEN
    CompletableFuture<Boolean> futureResult = resolver.get(mockDataFetchingEnvironment);
    futureResult.get(); // This should throw ExecutionException wrapping AuthorizationException
  }

  @Test(expectedExceptions = java.util.concurrent.ExecutionException.class)
  public void testDeleteWorkflowServiceFailure() throws Exception {
    // GIVEN
    DeleteActionWorkflowInput input = new DeleteActionWorkflowInput();
    input.setUrn(TEST_WORKFLOW_URN);

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN);
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);

    // Mock the service method to throw an exception
    doThrow(new RuntimeException("Service failure"))
        .when(mockActionWorkflowService)
        .deleteActionWorkflow(any(OperationContext.class), any(Urn.class));

    // WHEN
    CompletableFuture<Boolean> futureResult = resolver.get(mockDataFetchingEnvironment);
    futureResult.get(); // This should throw ExecutionException wrapping RuntimeException
  }

  @Test(expectedExceptions = java.util.concurrent.ExecutionException.class)
  public void testDeleteWorkflowWithInvalidUrn() throws Exception {
    // GIVEN
    DeleteActionWorkflowInput input = new DeleteActionWorkflowInput();
    input.setUrn("invalid-urn");

    // Mock environment and service behaviors
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN);
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);

    // WHEN
    CompletableFuture<Boolean> futureResult = resolver.get(mockDataFetchingEnvironment);
    futureResult.get(); // This should throw ExecutionException due to invalid URN
  }
}
