package com.linkedin.datahub.graphql.exception;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
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
}
