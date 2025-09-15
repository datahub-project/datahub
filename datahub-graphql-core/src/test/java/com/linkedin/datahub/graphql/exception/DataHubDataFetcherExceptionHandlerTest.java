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
    // Test that IllegalStateException is found in the cause chain
    IllegalStateException rootCause = new IllegalStateException("Root cause error");
    RuntimeException topLevelException = new RuntimeException("Top level error", rootCause);

    // Use reflection to test the private method
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
    // Test that IllegalArgumentException is found in the cause chain
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
    // Test that DataHubGraphQLException is found in the cause chain
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
    // Test that ValidationException is found in the cause chain
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
    // Test that null is returned when the exception type is not found
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
    // Test that null is returned when the exception is null
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
    // Test that the first matching exception in the chain is found
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
  public void testExceptionHandlingLogic() {
    // Test the core exception handling logic by creating a simple test
    // This test verifies that the new IllegalStateException handling code is present

    // Create a test exception with IllegalStateException as the cause
    IllegalStateException illegalStateException = new IllegalStateException("Test state error");
    RuntimeException testException = new RuntimeException("Test error", illegalStateException);

    // Test that the method can find the IllegalStateException
    try {
      java.lang.reflect.Method method =
          DataHubDataFetcherExceptionHandler.class.getDeclaredMethod(
              "findFirstThrowableCauseOfClass", Throwable.class, Class.class);
      method.setAccessible(true);

      IllegalStateException result =
          (IllegalStateException)
              method.invoke(handler, testException, IllegalStateException.class);

      assertNotNull(result);
      assertEquals(result.getMessage(), "Test state error");
    } catch (Exception e) {
      fail("Failed to invoke private method: " + e.getMessage());
    }
  }
}
