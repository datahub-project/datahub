package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.StoreExecutionRequestUploadLocationInput;
import com.linkedin.datahub.graphql.generated.StoreExecutionRequestUploadLocationResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Test
public class StoreExecutionRequestUploadLocationResolverTest {

  private static final Urn TEST_EXECUTION_REQUEST_URN =
      Urn.createFromTuple(Constants.EXECUTION_REQUEST_ENTITY_NAME, "test-id");
  private static final String TEST_LOCATION = "s3://my-bucket/path/to/artifacts";

  @Test
  public void testStoreUploadLocationSuccess() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = mock(QueryContext.class);
    OperationContext mockOperationContext = mock(OperationContext.class);

    StoreExecutionRequestUploadLocationInput input = new StoreExecutionRequestUploadLocationInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setLocation(TEST_LOCATION);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockContext.isAuthenticated()).thenReturn(true);
    when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.authorize(any(), any(), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));
    when(mockClient.ingestProposal(any(), any(), eq(false)))
        .thenReturn(TEST_EXECUTION_REQUEST_URN.toString());

    StoreExecutionRequestUploadLocationResolver resolver =
        new StoreExecutionRequestUploadLocationResolver(mockClient);

    // Execute
    StoreExecutionRequestUploadLocationResult result = resolver.get(mockEnv).join();

    // Verify
    assertTrue(result.getSuccess());
    verify(mockClient).ingestProposal(any(), any(), eq(false));
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testInvalidUrnThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = mock(QueryContext.class);
    OperationContext mockOperationContext = mock(OperationContext.class);

    StoreExecutionRequestUploadLocationInput input = new StoreExecutionRequestUploadLocationInput();
    input.setExecutionRequestUrn("urn:li:dataset:(test,test,test)"); // Wrong entity type
    input.setLocation(TEST_LOCATION);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockContext.isAuthenticated()).thenReturn(true);
    when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.authorize(any(), any(), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    StoreExecutionRequestUploadLocationResolver resolver =
        new StoreExecutionRequestUploadLocationResolver(mockClient);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testUnauthenticatedUserThrowsException() throws Exception {
    // Setup
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = mock(QueryContext.class);
    OperationContext mockOperationContext = mock(OperationContext.class);

    StoreExecutionRequestUploadLocationInput input = new StoreExecutionRequestUploadLocationInput();
    input.setExecutionRequestUrn(TEST_EXECUTION_REQUEST_URN.toString());
    input.setLocation(TEST_LOCATION);

    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockContext.isAuthenticated()).thenReturn(false); // Not authenticated
    when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.authorize(any(), any(), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.DENY, "message"));

    StoreExecutionRequestUploadLocationResolver resolver =
        new StoreExecutionRequestUploadLocationResolver(mockClient);

    // Execute - should throw exception
    resolver.get(mockEnv).join();
  }
}
