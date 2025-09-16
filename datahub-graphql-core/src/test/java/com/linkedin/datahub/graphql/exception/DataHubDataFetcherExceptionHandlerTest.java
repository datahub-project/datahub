package com.linkedin.datahub.graphql.exception;

import static org.testng.Assert.*;

import com.linkedin.metadata.entity.validation.ValidationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubDataFetcherExceptionHandlerTest {

  private DataHubDataFetcherExceptionHandler handler;

  @BeforeMethod
  public void setUp() {
    handler = new DataHubDataFetcherExceptionHandler();
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithIllegalStateException() {
    IllegalStateException rootCause = new IllegalStateException("Root cause error");
    RuntimeException topLevelException = new RuntimeException("Top level error", rootCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalStateException result =
          (IllegalStateException)
              method.invoke(handler, topLevelException, IllegalStateException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Root cause error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithIllegalArgumentException() {
    IllegalArgumentException rootCause = new IllegalArgumentException("Root cause error");
    RuntimeException topLevelException = new RuntimeException("Top level error", rootCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalArgumentException result =
          (IllegalArgumentException)
              method.invoke(handler, topLevelException, IllegalArgumentException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Root cause error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithDataHubGraphQLException() {
    DataHubGraphQLException rootCause =
        new DataHubGraphQLException("Root cause error", DataHubGraphQLErrorCode.UNAUTHORIZED);
    RuntimeException topLevelException = new RuntimeException("Top level error", rootCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      DataHubGraphQLException result =
          (DataHubGraphQLException)
              method.invoke(handler, topLevelException, DataHubGraphQLException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Root cause error");
      assertEquals(result.errorCode(), DataHubGraphQLErrorCode.UNAUTHORIZED);
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithValidationException() {
    ValidationException rootCause = new ValidationException("Root cause error");
    RuntimeException topLevelException = new RuntimeException("Top level error", rootCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      ValidationException result =
          (ValidationException)
              method.invoke(handler, topLevelException, ValidationException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Root cause error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testFindFirstThrowableCauseOfClassNotFound() {
    RuntimeException topLevelException = new RuntimeException("Top level error");

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalStateException result =
          (IllegalStateException)
              method.invoke(handler, topLevelException, IllegalStateException.class);

      assertNull(result);
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithNullException() {
    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalStateException result =
          (IllegalStateException) method.invoke(handler, null, IllegalStateException.class);

      assertNull(result);
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testFindFirstThrowableCauseOfClassWithMultipleCauses() {
    ValidationException validationCause = new ValidationException("Validation error");
    IllegalStateException illegalStateCause =
        new IllegalStateException("State error", validationCause);
    IllegalArgumentException illegalArgumentCause =
        new IllegalArgumentException("Argument error", illegalStateCause);
    RuntimeException topLevelException =
        new RuntimeException("Top level error", illegalArgumentCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalArgumentException result =
          (IllegalArgumentException)
              method.invoke(handler, topLevelException, IllegalArgumentException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Argument error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testExceptionPriorityHandling() {
    // Test that IllegalArgumentException takes priority over IllegalStateException
    IllegalStateException stateCause = new IllegalStateException("State error");
    IllegalArgumentException argCause = new IllegalArgumentException("Argument error", stateCause);
    RuntimeException topLevelException = new RuntimeException("Top level", argCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      // Should find IllegalArgumentException first (it's checked first in the handler)
      IllegalArgumentException result =
          (IllegalArgumentException)
              method.invoke(handler, topLevelException, IllegalArgumentException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Argument error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testExceptionPriorityHandlingWithDataHubGraphQLException() {
    // Test that DataHubGraphQLException takes priority over other exceptions
    ValidationException validationCause = new ValidationException("Validation error");
    IllegalStateException stateCause = new IllegalStateException("State error", validationCause);
    DataHubGraphQLException graphQLCause =
        new DataHubGraphQLException("GraphQL error", DataHubGraphQLErrorCode.UNAUTHORIZED);
    IllegalArgumentException argCause =
        new IllegalArgumentException("Argument error", graphQLCause);
    RuntimeException topLevelException = new RuntimeException("Top level", argCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      // Should find DataHubGraphQLException
      DataHubGraphQLException result =
          (DataHubGraphQLException)
              method.invoke(handler, topLevelException, DataHubGraphQLException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "GraphQL error");
      assertEquals(result.errorCode(), DataHubGraphQLErrorCode.UNAUTHORIZED);
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testExceptionPriorityHandlingWithValidationException() {
    // Test that ValidationException is found in the cause chain
    ValidationException validationCause = new ValidationException("Validation error");
    RuntimeException topLevelException = new RuntimeException("Top level", validationCause);

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      // Should find ValidationException
      ValidationException result =
          (ValidationException)
              method.invoke(handler, topLevelException, ValidationException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Validation error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }

  @Test
  public void testExceptionChainWithNullCause() {
    // Test exception chain that ends with null cause
    RuntimeException topLevelException = new RuntimeException("Top level error");
    // No cause set, so getCause() will return null

    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalArgumentException result =
          (IllegalArgumentException)
              method.invoke(handler, topLevelException, IllegalArgumentException.class);

      assertNull(result);
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }
}
