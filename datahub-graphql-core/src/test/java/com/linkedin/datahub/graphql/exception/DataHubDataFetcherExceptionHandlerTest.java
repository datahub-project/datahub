package com.linkedin.datahub.graphql.exception;

import static org.testng.Assert.*;

import com.linkedin.metadata.entity.validation.ValidationException;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubDataFetcherExceptionHandlerTest {

  private DataHubDataFetcherExceptionHandler handler;
  private DataFetcherExceptionHandlerParameters mockParameters;
  private SourceLocation mockSourceLocation;
  private ResultPath mockPath;

  @BeforeMethod
  public void setup() {
    handler = new DataHubDataFetcherExceptionHandler();
    mockParameters = Mockito.mock(DataFetcherExceptionHandlerParameters.class);
    mockSourceLocation = Mockito.mock(SourceLocation.class);
    mockPath = Mockito.mock(ResultPath.class);

    Mockito.when(mockParameters.getSourceLocation()).thenReturn(mockSourceLocation);
    Mockito.when(mockParameters.getPath()).thenReturn(mockPath);
  }

  @Test
  public void testHandleIllegalArgumentException() throws ExecutionException, InterruptedException {
    IllegalArgumentException exception = new IllegalArgumentException("Invalid argument");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Invalid argument");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testHandleIllegalStateException() throws ExecutionException, InterruptedException {
    IllegalStateException exception = new IllegalStateException("Invalid state");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Invalid state");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleDataHubGraphQLException() throws ExecutionException, InterruptedException {
    DataHubGraphQLException exception =
        new DataHubGraphQLException("GraphQL error", DataHubGraphQLErrorCode.NOT_FOUND);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "GraphQL error");
    assertEquals(error.getErrorCode(), 404);
  }

  @Test
  public void testHandleDataHubGraphQLExceptionUnauthorized() throws ExecutionException, InterruptedException {
    DataHubGraphQLException exception =
        new DataHubGraphQLException("Unauthorized access", DataHubGraphQLErrorCode.UNAUTHORIZED);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Unauthorized access");
    assertEquals(error.getErrorCode(), 403);
  }

  @Test
  public void testHandleDataHubGraphQLExceptionConflict() throws ExecutionException, InterruptedException {
    DataHubGraphQLException exception =
        new DataHubGraphQLException("Resource conflict", DataHubGraphQLErrorCode.CONFLICT);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Resource conflict");
    assertEquals(error.getErrorCode(), 409);
  }

  @Test
  public void testHandleValidationException() throws ExecutionException, InterruptedException {
    ValidationException exception = new ValidationException("Validation failed");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Validation failed");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testHandleRuntimeException() throws ExecutionException, InterruptedException {
    RuntimeException exception = new RuntimeException("Runtime error");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Runtime error");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleNestedIllegalArgumentException() throws ExecutionException, InterruptedException {
    IllegalArgumentException cause = new IllegalArgumentException("Nested invalid argument");
    RuntimeException wrapper = new RuntimeException("Wrapper exception", cause);
    Mockito.when(mockParameters.getException()).thenReturn(wrapper);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Nested invalid argument");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testHandleNestedIllegalStateException() throws ExecutionException, InterruptedException {
    IllegalStateException cause = new IllegalStateException("Nested state error");
    RuntimeException wrapper = new RuntimeException("Wrapper exception", cause);
    Mockito.when(mockParameters.getException()).thenReturn(wrapper);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Nested state error");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleNestedDataHubGraphQLException() throws ExecutionException, InterruptedException {
    DataHubGraphQLException cause =
        new DataHubGraphQLException("Nested GraphQL error", DataHubGraphQLErrorCode.UNAUTHORIZED);
    RuntimeException wrapper = new RuntimeException("Wrapper exception", cause);
    Mockito.when(mockParameters.getException()).thenReturn(wrapper);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Nested GraphQL error");
    assertEquals(error.getErrorCode(), 403);
  }

  @Test
  public void testHandleNestedValidationException() throws ExecutionException, InterruptedException {
    ValidationException cause = new ValidationException("Nested validation failed");
    RuntimeException wrapper = new RuntimeException("Wrapper exception", cause);
    Mockito.when(mockParameters.getException()).thenReturn(wrapper);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Nested validation failed");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testHandleUnknownException() throws ExecutionException, InterruptedException {
    Exception exception = new Exception("Unknown error");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "An unknown error occurred.");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleRuntimeExceptionWithNullMessage() throws ExecutionException, InterruptedException {
    RuntimeException exception = new RuntimeException((String) null);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertNull(error.getMessage());
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testPriorityOrderOfExceptionHandling() throws ExecutionException, InterruptedException {
    // DataHubGraphQLException should take priority over IllegalArgumentException
    DataHubGraphQLException dataHubException =
        new DataHubGraphQLException("DataHub error", DataHubGraphQLErrorCode.CONFLICT);
    IllegalArgumentException illegalArgException =
        new IllegalArgumentException("Illegal arg", dataHubException);
    Mockito.when(mockParameters.getException()).thenReturn(illegalArgException);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "DataHub error");
    assertEquals(error.getErrorCode(), 409);
  }

  @Test
  public void testComplexNestedExceptionHierarchy() throws ExecutionException, InterruptedException {
    // Create a complex nested exception hierarchy
    ValidationException validationCause = new ValidationException("Validation error");
    IllegalStateException illegalStateCause = new IllegalStateException("State error", validationCause);
    DataHubGraphQLException graphQLCause =
        new DataHubGraphQLException("GraphQL error", DataHubGraphQLErrorCode.UNAUTHORIZED);
    IllegalArgumentException illegalArgCause =
        new IllegalArgumentException("Argument error", graphQLCause);
    RuntimeException topLevelException = new RuntimeException("Top level", illegalArgCause);
    
    Mockito.when(mockParameters.getException()).thenReturn(topLevelException);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    // DataHubGraphQLException should be found and used
    assertEquals(error.getMessage(), "GraphQL error");
    assertEquals(error.getErrorCode(), 403);
  }

  @Test
  public void testMultipleValidationExceptionsInChain() throws ExecutionException, InterruptedException {
    ValidationException deepValidation = new ValidationException("Deep validation error");
    IllegalStateException middleException = new IllegalStateException("Middle error", deepValidation);
    ValidationException topValidation = new ValidationException("Top validation error", middleException);
    
    Mockito.when(mockParameters.getException()).thenReturn(topValidation);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    // Should use the first ValidationException found
    assertEquals(error.getMessage(), "Top validation error");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testExceptionWithNoCause() throws ExecutionException, InterruptedException {
    RuntimeException exception = new RuntimeException("No cause exception");
    // No cause set, so getCause() will return null
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "No cause exception");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleExceptionWithEmptyMessage() throws ExecutionException, InterruptedException {
    RuntimeException exception = new RuntimeException("");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleExecutionExceptionWithNestedCause() throws ExecutionException, InterruptedException {
    IllegalArgumentException rootCause = new IllegalArgumentException("Root illegal argument");
    ExecutionException executionException = new ExecutionException("Execution failed", rootCause);
    Mockito.when(mockParameters.getException()).thenReturn(executionException);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Root illegal argument");
    assertEquals(error.getErrorCode(), 400);
  }
}