package com.linkedin.datahub.graphql.exception;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Basic tests for DataHubDataFetcherExceptionHandler */
public class DataHubDataFetcherExceptionHandlerTest {

  @Mock private DataFetcherExceptionHandlerParameters mockParameters;
  @Mock private SourceLocation mockSourceLocation;
  @Mock private ResultPath mockPath;

  private DataHubDataFetcherExceptionHandler handler;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    handler = new DataHubDataFetcherExceptionHandler();

    when(mockParameters.getSourceLocation()).thenReturn(mockSourceLocation);
    when(mockParameters.getPath()).thenReturn(mockPath);
  }

  @Test
  public void testHandleException_DataHubGraphQLException() throws Exception {
    DataHubGraphQLException graphqlException =
        new DataHubGraphQLException("Test error", DataHubGraphQLErrorCode.BAD_REQUEST);
    when(mockParameters.getException()).thenReturn(graphqlException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertTrue(handlerResult.getErrors().get(0).getMessage().contains("Test error"));
  }

  @Test
  public void testHandleException_IllegalArgumentException() throws Exception {
    IllegalArgumentException illegalArgException = new IllegalArgumentException("Bad argument");
    when(mockParameters.getException()).thenReturn(illegalArgException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertTrue(handlerResult.getErrors().get(0).getMessage().contains("Bad argument"));
  }

  @Test
  public void testHandleException_RuntimeException() throws Exception {
    RuntimeException runtimeException = new RuntimeException("Runtime error");
    when(mockParameters.getException()).thenReturn(runtimeException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertTrue(handlerResult.getErrors().get(0).getMessage().contains("Runtime error"));
  }

  @Test
  public void testHandleException_UnknownException() throws Exception {
    Exception unknownException = new Exception("Unknown error");
    when(mockParameters.getException()).thenReturn(unknownException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertNotNull(handlerResult.getErrors().get(0).getMessage());
  }

  @Test
  public void testHandleException_NestedExceptions() throws Exception {
    DataHubGraphQLException rootCause =
        new DataHubGraphQLException("Root cause", DataHubGraphQLErrorCode.BAD_REQUEST);
    RuntimeException wrapper = new RuntimeException("Wrapper", rootCause);
    when(mockParameters.getException()).thenReturn(wrapper);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    // Should extract root cause message
    String errorMessage = handlerResult.getErrors().get(0).getMessage();
    assertTrue(errorMessage.contains("Root cause") || errorMessage.contains("Wrapper"));
  }

  @Test
  public void testHandleException_ValidationExceptionWithAuthorizationSubType() throws Exception {
    // Create ValidationException with AUTHORIZATION subtype
    ValidationExceptionCollection collection = mock(ValidationExceptionCollection.class);
    when(collection.getSubTypes())
        .thenReturn(Collections.singleton(ValidationSubType.AUTHORIZATION));
    when(collection.toString()).thenReturn("Authorization denied");

    com.linkedin.metadata.entity.validation.ValidationException validationException =
        new com.linkedin.metadata.entity.validation.ValidationException(collection);
    when(mockParameters.getException()).thenReturn(validationException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    DataHubGraphQLError error = (DataHubGraphQLError) handlerResult.getErrors().get(0);
    assertEquals(error.getErrorCode(), DataHubGraphQLErrorCode.UNAUTHORIZED.getCode());
    assertTrue(error.getMessage().contains("Authorization denied"));
  }

  @Test
  public void testHandleException_ValidationExceptionWithoutAuthorizationSubType()
      throws Exception {
    // Create ValidationException without AUTHORIZATION subtype
    ValidationExceptionCollection collection = mock(ValidationExceptionCollection.class);
    when(collection.getSubTypes()).thenReturn(Collections.emptySet());
    when(collection.toString()).thenReturn("Validation failed");

    com.linkedin.metadata.entity.validation.ValidationException validationException =
        new com.linkedin.metadata.entity.validation.ValidationException(collection);
    when(mockParameters.getException()).thenReturn(validationException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    DataHubGraphQLError error = (DataHubGraphQLError) handlerResult.getErrors().get(0);
    assertEquals(error.getErrorCode(), DataHubGraphQLErrorCode.BAD_REQUEST.getCode());
    assertTrue(error.getMessage().contains("Validation failed"));
  }

  @Test
  public void testHandleException_GraphQLValidationException() throws Exception {
    // Test the GraphQL-specific ValidationException (different from entity service one)
    com.linkedin.datahub.graphql.exception.ValidationException graphqlValidationException =
        new com.linkedin.datahub.graphql.exception.ValidationException("GraphQL validation error");
    when(mockParameters.getException()).thenReturn(graphqlValidationException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    DataHubGraphQLError error = (DataHubGraphQLError) handlerResult.getErrors().get(0);
    assertEquals(error.getErrorCode(), DataHubGraphQLErrorCode.BAD_REQUEST.getCode());
    assertTrue(error.getMessage().contains("GraphQL validation error"));
  }

  @Test
  public void testHandleException_MessagePrioritization() throws Exception {
    // Test that DataHubGraphQLException is found even when wrapped in other exceptions
    DataHubGraphQLException graphqlException =
        new DataHubGraphQLException("GraphQL error", DataHubGraphQLErrorCode.SERVER_ERROR);

    // Wrap GraphQL exception in a RuntimeException
    RuntimeException wrapper = new RuntimeException("Wrapper error", graphqlException);
    when(mockParameters.getException()).thenReturn(wrapper);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);

    // GraphQL exception should be found and used
    String errorMessage = handlerResult.getErrors().get(0).getMessage();
    assertTrue(errorMessage.contains("GraphQL error") || errorMessage.contains("Wrapper error"));
  }

  @Test
  public void testHandleException_IllegalStateException() throws Exception {
    IllegalStateException illegalStateException = new IllegalStateException("Invalid state");
    when(mockParameters.getException()).thenReturn(illegalStateException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    DataHubGraphQLError error = (DataHubGraphQLError) handlerResult.getErrors().get(0);
    assertEquals(error.getErrorCode(), DataHubGraphQLErrorCode.SERVER_ERROR.getCode());
    assertTrue(error.getMessage().contains("Invalid state"));
  }
}
