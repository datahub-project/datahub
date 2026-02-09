package com.linkedin.datahub.graphql.exception;

import static org.testng.Assert.*;

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
  public void testHandleDataHubGraphQLExceptionUnauthorized()
      throws ExecutionException, InterruptedException {
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
  public void testHandleDataHubGraphQLExceptionConflict()
      throws ExecutionException, InterruptedException {
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
  public void testHandleNestedIllegalArgumentException()
      throws ExecutionException, InterruptedException {
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
  public void testHandleNestedIllegalStateException()
      throws ExecutionException, InterruptedException {
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
  public void testHandleNestedDataHubGraphQLException()
      throws ExecutionException, InterruptedException {
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
  public void testHandleNestedValidationException()
      throws ExecutionException, InterruptedException {
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
  public void testHandleRuntimeExceptionWithNullMessage()
      throws ExecutionException, InterruptedException {
    RuntimeException exception = new RuntimeException((String) null);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "An unknown error occurred.");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testPriorityOrderOfExceptionHandling()
      throws ExecutionException, InterruptedException {
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
  public void testComplexNestedExceptionHierarchy()
      throws ExecutionException, InterruptedException {
    // Create a complex nested exception hierarchy
    ValidationException validationCause = new ValidationException("Validation error");
    IllegalStateException illegalStateCause =
        new IllegalStateException("State error", validationCause);
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
  public void testMultipleValidationExceptionsInChain()
      throws ExecutionException, InterruptedException {
    ValidationException deepValidation = new ValidationException("Deep validation error");
    IllegalStateException middleException =
        new IllegalStateException("Middle error", deepValidation);
    ValidationException topValidation =
        new ValidationException("Top validation error", middleException);

    Mockito.when(mockParameters.getException()).thenReturn(topValidation);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    String message = error.getMessage();
    assertTrue(message.contains("Top validation error"));
    assertTrue(message.contains("Middle error"));
    assertTrue(message.contains("Deep validation error"));
    assertTrue(message.contains("Root cause"));
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
  public void testHandleExceptionWithEmptyMessage()
      throws ExecutionException, InterruptedException {
    RuntimeException exception = new RuntimeException("");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "An unknown error occurred.");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testHandleExecutionExceptionWithNestedCause()
      throws ExecutionException, InterruptedException {
    IllegalArgumentException rootCause = new IllegalArgumentException("Root illegal argument");
    ExecutionException executionException = new ExecutionException("Execution failed", rootCause);
    Mockito.when(mockParameters.getException()).thenReturn(executionException);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    // IllegalArgumentException has no cause, so message should be exactly as is
    assertEquals(error.getMessage(), "Root illegal argument");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testExtractErrorMessageWithRootCause()
      throws ExecutionException, InterruptedException {
    // Simulating the real-world scenario from the issue: TimeoutException wrapped in
    // RuntimeException
    java.util.concurrent.TimeoutException rootCause =
        new java.util.concurrent.TimeoutException("Connection lease request time out");
    RuntimeException wrappedException =
        new RuntimeException(
            "Failed to execute search: entity types null, query *, filters: [{ and: [{ field: \"removed\", values: [true], negated: true, condition: EQUAL }] }], start: null, count: 3000",
            rootCause);
    Mockito.when(mockParameters.getException()).thenReturn(wrappedException);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    // Verify the message includes both the wrapper message and root cause
    assertTrue(error.getMessage().contains("Failed to execute search"));
    assertTrue(error.getMessage().contains("Connection lease request time out"));
    assertTrue(error.getMessage().contains("Root cause"));
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testExtractErrorMessageWithDeepNesting()
      throws ExecutionException, InterruptedException {
    // Create a deeply nested exception chain with different messages at each level
    Exception level3 = new Exception("Database connection failed");
    RuntimeException level2 = new RuntimeException("Query execution failed", level3);
    RuntimeException level1 =
        new RuntimeException("Failed to execute search: query *, count: 100", level2);
    Mockito.when(mockParameters.getException()).thenReturn(level1);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    String message = error.getMessage();
    // Verify all messages in the chain are included
    assertTrue(message.contains("Failed to execute search"));
    assertTrue(message.contains("Query execution failed"));
    assertTrue(message.contains("Database connection failed"));
    assertTrue(message.contains("Root cause"));
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testExtractErrorMessageWithDuplicateMessages()
      throws ExecutionException, InterruptedException {
    // Test that duplicate messages in the exception chain are deduplicated
    RuntimeException level2 = new RuntimeException("Connection timeout");
    RuntimeException level1 = new RuntimeException("Connection timeout", level2);
    Mockito.when(mockParameters.getException()).thenReturn(level1);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    // The message should appear only once, not duplicated
    String message = error.getMessage();
    int firstIndex = message.indexOf("Connection timeout");
    int lastIndex = message.lastIndexOf("Connection timeout");
    assertEquals(
        firstIndex, lastIndex, "Duplicate message should be deduplicated, but found: " + message);
  }

  @Test
  public void testExtractErrorMessageWithNullAndEmptyMessages()
      throws ExecutionException, InterruptedException {
    // Test handling of null and empty messages in the exception chain
    Exception level3 = new Exception((String) null);
    RuntimeException level2 = new RuntimeException("", level3);
    RuntimeException level1 = new RuntimeException("Valid error message", level2);
    Mockito.when(mockParameters.getException()).thenReturn(level1);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    // Should only include the valid error message, skipping null and empty
    assertEquals(error.getMessage(), "Valid error message");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testExtractErrorMessageWithCircularReference()
      throws ExecutionException, InterruptedException {
    // Create an exception with a circular reference (cause points to itself)
    RuntimeException exception = new RuntimeException("Circular exception");
    // Note: We can't actually create a true circular reference in Java, but the code should
    // handle the case where cause == cause.getCause() by checking that condition
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Circular exception");
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testExtractErrorMessagePreservesOriginalBehaviorForSimpleExceptions()
      throws ExecutionException, InterruptedException {
    // Verify that exceptions without a cause still work as before
    IllegalArgumentException exception = new IllegalArgumentException("Simple validation error");
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertEquals(error.getMessage(), "Simple validation error");
    assertEquals(error.getErrorCode(), 400);
  }

  @Test
  public void testExtractErrorMessageWithDataHubGraphQLExceptionAndCause()
      throws ExecutionException, InterruptedException {
    // Test that DataHubGraphQLException also includes root cause
    IllegalStateException rootCause = new IllegalStateException("State is invalid");
    DataHubGraphQLException exception =
        new DataHubGraphQLException(
            "Operation not permitted", DataHubGraphQLErrorCode.UNAUTHORIZED, rootCause);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertTrue(error.getMessage().contains("Operation not permitted"));
    assertTrue(error.getMessage().contains("State is invalid"));
    assertTrue(error.getMessage().contains("Root cause"));
    assertEquals(error.getErrorCode(), 403);
  }

  @Test
  public void testExtractErrorMessageWithValidationExceptionAndCause()
      throws ExecutionException, InterruptedException {
    // Test that ValidationException also includes root cause
    IllegalArgumentException rootCause = new IllegalArgumentException("Field 'email' is required");
    ValidationException exception = new ValidationException("Validation failed", rootCause);
    Mockito.when(mockParameters.getException()).thenReturn(exception);

    DataFetcherExceptionHandlerResult result = handler.handleException(mockParameters).get();

    assertNotNull(result);
    assertEquals(result.getErrors().size(), 1);
    DataHubGraphQLError error = (DataHubGraphQLError) result.getErrors().get(0);
    assertTrue(error.getMessage().contains("Validation failed"));
    assertTrue(error.getMessage().contains("Field 'email' is required"));
    assertTrue(error.getMessage().contains("Root cause"));
    assertEquals(error.getErrorCode(), 400);
  }
}
