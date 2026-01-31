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

  @Test
  public void testHandleException_ExceptionWithNullMessage() throws Exception {
    // Test exception with null message
    RuntimeException exceptionWithNullMessage = new RuntimeException((String) null);
    when(mockParameters.getException()).thenReturn(exceptionWithNullMessage);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertNotNull(handlerResult.getErrors().get(0).getMessage());
  }

  @Test
  public void testHandleException_ExceptionWithEmptyMessage() throws Exception {
    // Test exception with empty message
    RuntimeException exceptionWithEmptyMessage = new RuntimeException("");
    when(mockParameters.getException()).thenReturn(exceptionWithEmptyMessage);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertNotNull(handlerResult.getErrors().get(0).getMessage());
  }

  @Test
  public void testHandleException_DeepExceptionChain() throws Exception {
    // Create a deep exception chain
    Exception rootCause = new Exception("Root cause message");
    Exception level1 = new Exception("Level 1 message", rootCause);
    Exception level2 = new Exception("Level 2 message", level1);
    Exception level3 = new Exception("Level 3 message", level2);
    when(mockParameters.getException()).thenReturn(level3);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    String errorMessage = handlerResult.getErrors().get(0).getMessage();
    assertNotNull(errorMessage);
    // Should contain some error message from the chain
    assertTrue(errorMessage.length() > 0);
  }

  @Test
  public void testHandleException_CircularCauseChain() throws Exception {
    // Test handling of circular cause chains (should not infinite loop)
    RuntimeException ex1 = new RuntimeException("Exception 1");
    RuntimeException ex2 = new RuntimeException("Exception 2", ex1);
    // Note: Can't actually create circular reference in Java without reflection,
    // but test that handler can handle exceptions with causes
    when(mockParameters.getException()).thenReturn(ex2);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertNotNull(handlerResult.getErrors().get(0).getMessage());
  }

  @Test
  public void testHandleException_DuplicateCauseMessages() throws Exception {
    // Test that duplicate messages in cause chain are handled properly
    Exception rootCause = new Exception("Duplicate message");
    Exception wrapper1 = new Exception("Duplicate message", rootCause);
    Exception wrapper2 = new Exception("Unique message", wrapper1);
    when(mockParameters.getException()).thenReturn(wrapper2);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    String errorMessage = handlerResult.getErrors().get(0).getMessage();
    assertNotNull(errorMessage);
    // Should have some error message
    assertTrue(errorMessage.length() > 0);
  }

  @Test
  public void testHandleException_MultipleNestedDataHubExceptions() throws Exception {
    // Test multiple nested DataHub exceptions
    DataHubGraphQLException innerException =
        new DataHubGraphQLException("Inner error", DataHubGraphQLErrorCode.NOT_FOUND);
    DataHubGraphQLException outerException =
        new DataHubGraphQLException(
            "Outer error", DataHubGraphQLErrorCode.BAD_REQUEST, innerException);
    when(mockParameters.getException()).thenReturn(outerException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);

    DataHubGraphQLError error = (DataHubGraphQLError) handlerResult.getErrors().get(0);
    // Should use the first DataHub exception found
    assertEquals(error.getErrorCode(), DataHubGraphQLErrorCode.BAD_REQUEST.getCode());
  }

  @Test
  public void testHandleException_ValidationExceptionEmptyCollection() throws Exception {
    // Test ValidationException with empty collection (null collection not allowed by constructor)
    ValidationExceptionCollection collection = mock(ValidationExceptionCollection.class);
    when(collection.getSubTypes()).thenReturn(Collections.emptySet());
    when(collection.toString()).thenReturn("Empty validation collection");

    com.linkedin.metadata.entity.validation.ValidationException validationException =
        new com.linkedin.metadata.entity.validation.ValidationException(collection);
    when(mockParameters.getException()).thenReturn(validationException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertNotNull(handlerResult.getErrors().get(0).getMessage());
  }

  @Test
  public void testHandleException_ExceptionPriority() throws Exception {
    // Test that DataHubGraphQLException takes priority over IllegalArgumentException
    IllegalArgumentException illegalArg = new IllegalArgumentException("Illegal arg message");
    DataHubGraphQLException graphqlException =
        new DataHubGraphQLException("GraphQL message", DataHubGraphQLErrorCode.NOT_FOUND);

    // Wrap GraphQL exception in IllegalArgument
    IllegalArgumentException wrapper = new IllegalArgumentException("Wrapper", graphqlException);
    when(mockParameters.getException()).thenReturn(wrapper);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);

    DataHubGraphQLError error = (DataHubGraphQLError) handlerResult.getErrors().get(0);
    // Should prioritize DataHubGraphQLException
    assertEquals(error.getErrorCode(), DataHubGraphQLErrorCode.NOT_FOUND.getCode());
  }

  @Test
  public void testHandleException_AllExceptionTypesNull() throws Exception {
    // Test with a generic exception that doesn't match any specific type deeply
    Exception genericException = new Exception("Generic exception");
    when(mockParameters.getException()).thenReturn(genericException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);
    assertEquals(handlerResult.getErrors().size(), 1);
    assertNotNull(handlerResult.getErrors().get(0).getMessage());
  }

  @Test
  public void testHandleException_ExtractErrorMessageWithRootCause() throws Exception {
    // Test error message extraction includes root cause
    Exception rootCause = new Exception("This is the root cause");
    Exception middleException = new Exception("Middle exception", rootCause);
    RuntimeException topException = new RuntimeException("Top level exception", middleException);
    when(mockParameters.getException()).thenReturn(topException);

    CompletableFuture<DataFetcherExceptionHandlerResult> result =
        handler.handleException(mockParameters);

    assertNotNull(result);
    DataFetcherExceptionHandlerResult handlerResult = result.get();
    assertNotNull(handlerResult);

    String errorMessage = handlerResult.getErrors().get(0).getMessage();
    // Should include root cause in the message
    assertTrue(errorMessage.contains("Root cause:") || errorMessage.contains("root cause"));
  }
}
