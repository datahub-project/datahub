package com.linkedin.metadata.resources.restli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.datahub.util.exception.RetryLimitReached;
import com.linkedin.metadata.dao.throttle.DatabaseTransactionConflictRestLiServiceException;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.exception.ActorAccessException;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class RestliUtilsTest {

  @Test
  public void testToTask_DatabaseTransactionConflict_Returns503() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw conflict;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(thrown.getCause(), conflict);
    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertTrue(thrown.hasErrorDetails());
    assertEquals(thrown.getErrorDetails().get("retryable"), true);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testToTask_DatabaseTransactionConflict_customRetryAfter() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001", null, 9L);

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw conflict;
                    }));

    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "9");
  }

  @Test
  public void testToTask_WrappedDatabaseTransactionConflict_Returns503() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40P01");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw new RuntimeException("wrapper", conflict);
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(thrown.getCause(), conflict);
    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertTrue(thrown.hasErrorDetails());
    assertEquals(thrown.getErrorDetails().get("retryable"), true);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testToTask_DoubleWrappedDatabaseTransactionConflict_Returns503() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw new RuntimeException(
                          "outer", new RuntimeException("inner", conflict));
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(thrown.getCause(), conflict);
    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertTrue(thrown.hasErrorDetails());
    assertEquals(thrown.getErrorDetails().get("retryable"), true);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testToTask_GenericRuntime_Returns500() {
    RuntimeException boom = new RuntimeException("boom");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw boom;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_500_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testToTask_PlainRetryLimitReached_Returns500_Not503() {
    RetryLimitReached retryExhausted = new RetryLimitReached("Failed to add after 3 retries");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw retryExhausted;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_500_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testToTask_ConflictSubclassOfRetryLimitReached_Returns503() {
    // DatabaseTransactionConflictException extends RetryLimitReached — cause walk must match
    // the subclass before falling through to generic 500.
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");
    RuntimeException wrapper = new RuntimeException("wrapper", conflict);

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw wrapper;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
  }

  @Test
  public void testAsSessionMapsActorAccessExceptionToForbidden() {
    OperationContext systemOpContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Authentication auth = new Authentication(new Actor(ActorType.USER, "test"), "credentials");
    RequestContext.RequestContextBuilder requestContext =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:test", null, "test", java.util.List.of());

    try (MockedStatic<OperationContext> mocked = mockStatic(OperationContext.class)) {
      mocked
          .when(
              () ->
                  OperationContext.asSession(
                      eq(systemOpContext),
                      eq(requestContext),
                      any(Authorizer.class),
                      eq(auth),
                      anyBoolean()))
          .thenThrow(new ActorAccessException("actor denied"));

      RestLiServiceException thrown =
          expectThrows(
              RestLiServiceException.class,
              () ->
                  RestliUtils.asSession(
                      systemOpContext, requestContext, Authorizer.EMPTY, auth, true));

      assertEquals(thrown.getStatus(), HttpStatus.S_403_FORBIDDEN);
      assertEquals(thrown.getMessage(), "actor denied");
    }
  }

  @Test
  public void testToTaskMapsDirectActorAccessExceptionToForbidden() {
    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw new ActorAccessException("direct deny");
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_403_FORBIDDEN);
    assertEquals(thrown.getMessage(), "direct deny");
  }
}
