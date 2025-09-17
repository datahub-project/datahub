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
    assertNull(error.getMessage());
    assertEquals(error.getErrorCode(), 500);
  }

  @Test
  public void testPriorityOrderOfExceptionHandling()
      throws ExecutionException, InterruptedException {
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
  public void testFindFirstThrowableCauseOfClass() {
    IllegalArgumentException rootCause = new IllegalArgumentException("Root cause");
    RuntimeException middleCause = new RuntimeException("Middle cause", rootCause);
    Exception topException = new Exception("Top exception", middleCause);

    IllegalArgumentException found =
        handler.findFirstThrowableCauseOfClass(topException, IllegalArgumentException.class);
    assertNotNull(found);
    assertEquals(found.getMessage(), "Root cause");

    ValidationException notFound =
        handler.findFirstThrowableCauseOfClass(topException, ValidationException.class);
    assertNull(notFound);

    RuntimeException runtimeFound =
        handler.findFirstThrowableCauseOfClass(topException, RuntimeException.class);
    assertNotNull(runtimeFound);
    assertEquals(runtimeFound.getMessage(), "Middle cause");
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithNullThrowable() {
    IllegalArgumentException result =
        handler.findFirstThrowableCauseOfClass(null, IllegalArgumentException.class);
    assertNull(result);
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithSameClass() {
    IllegalArgumentException exception = new IllegalArgumentException("Direct match");

    IllegalArgumentException result =
        handler.findFirstThrowableCauseOfClass(exception, IllegalArgumentException.class);
    assertNotNull(result);
    assertEquals(result.getMessage(), "Direct match");
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithNoCause() {
    RuntimeException exception = new RuntimeException("No cause");

    IllegalArgumentException result =
        handler.findFirstThrowableCauseOfClass(exception, IllegalArgumentException.class);
    assertNull(result);
  }
}
