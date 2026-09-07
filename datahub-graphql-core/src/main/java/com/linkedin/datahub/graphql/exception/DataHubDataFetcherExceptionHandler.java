package com.linkedin.datahub.graphql.exception;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import graphql.PublicApi;
import graphql.execution.DataFetcherExceptionHandler;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@PublicApi
@Slf4j
public class DataHubDataFetcherExceptionHandler implements DataFetcherExceptionHandler {

  private static final String DEFAULT_ERROR_MESSAGE = "An unknown error occurred.";

  /**
   * Priority (first match wins via cause walk): {@link DataHubGraphQLException} → {@link
   * ValidationException} → {@link IllegalArgumentException} → {@link
   * DatabaseTransactionConflictException} → {@link IllegalStateException} → {@link
   * RuntimeException} → fallback. Conflict must stay above {@code RuntimeException} because {@code
   * DatabaseTransactionConflictException} extends {@code RetryLimitReached} (a RuntimeException).
   */
  @Override
  public CompletableFuture<DataFetcherExceptionHandlerResult> handleException(
      DataFetcherExceptionHandlerParameters handlerParameters) {
    Throwable exception = handlerParameters.getException();
    SourceLocation sourceLocation = handlerParameters.getSourceLocation();
    ResultPath path = handlerParameters.getPath();

    DataHubGraphQLException graphQLException =
        findFirstThrowableCauseOfClass(exception, DataHubGraphQLException.class);
    if (graphQLException != null) {
      log.error("Failed to execute", graphQLException);
      return completedResult(
          extractErrorMessage(graphQLException),
          graphQLException.errorCode(),
          path,
          sourceLocation);
    }

    ValidationException validationException =
        findFirstThrowableCauseOfClass(exception, ValidationException.class);
    if (validationException != null) {
      log.error("Failed to execute", validationException);
      return completedResult(
          extractErrorMessage(validationException),
          DataHubGraphQLErrorCode.BAD_REQUEST,
          path,
          sourceLocation);
    }

    IllegalArgumentException illException =
        findFirstThrowableCauseOfClass(exception, IllegalArgumentException.class);
    if (illException != null) {
      log.error("Failed to execute", illException);
      return completedResult(
          extractErrorMessage(illException),
          DataHubGraphQLErrorCode.BAD_REQUEST,
          path,
          sourceLocation);
    }

    DatabaseTransactionConflictException conflictException =
        findFirstThrowableCauseOfClass(exception, DatabaseTransactionConflictException.class);
    if (conflictException != null) {
      log.warn("Failed to execute", conflictException);
      String top = conflictException.getMessage();
      String message = (top != null && !top.isEmpty()) ? top : DEFAULT_ERROR_MESSAGE;
      return completedResult(
          message, DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE, path, sourceLocation);
    }

    IllegalStateException illegalStateException =
        findFirstThrowableCauseOfClass(exception, IllegalStateException.class);
    if (illegalStateException != null) {
      log.error("Failed to execute", illegalStateException);
      return completedResult(
          extractErrorMessage(illegalStateException),
          DataHubGraphQLErrorCode.SERVER_ERROR,
          path,
          sourceLocation);
    }

    RuntimeException runtimeException =
        findFirstThrowableCauseOfClass(exception, RuntimeException.class);
    if (runtimeException != null) {
      log.error("Failed to execute", runtimeException);
      return completedResult(
          extractErrorMessage(runtimeException),
          DataHubGraphQLErrorCode.SERVER_ERROR,
          path,
          sourceLocation);
    }

    log.error("Failed to execute", exception);
    return completedResult(
        DEFAULT_ERROR_MESSAGE, DataHubGraphQLErrorCode.SERVER_ERROR, path, sourceLocation);
  }

  private static <T extends Throwable> T findFirstThrowableCauseOfClass(
      Throwable throwable, Class<T> clazz) {
    while (throwable != null) {
      if (clazz.isInstance(throwable)) {
        return clazz.cast(throwable);
      }
      Throwable cause = throwable.getCause();
      if (cause == null || cause == throwable) {
        break;
      }
      throwable = cause;
    }
    return null;
  }

  private CompletableFuture<DataFetcherExceptionHandlerResult> completedResult(
      String message,
      DataHubGraphQLErrorCode errorCode,
      ResultPath path,
      SourceLocation sourceLocation) {
    DataHubGraphQLError error = new DataHubGraphQLError(message, path, sourceLocation, errorCode);
    return CompletableFuture.completedFuture(
        DataFetcherExceptionHandlerResult.newResult().error(error).build());
  }

  /**
   * Extracts a comprehensive error message including root cause information. Walks the exception
   * chain to find the deepest cause with a meaningful message.
   *
   * @param exception The exception to extract messages from
   * @return A message containing both the exception message and root cause messages
   */
  private String extractErrorMessage(Throwable exception) {
    StringBuilder message = new StringBuilder();

    String topLevelMessage = exception.getMessage();
    if (topLevelMessage != null && !topLevelMessage.isEmpty()) {
      message.append(topLevelMessage);
    }

    Throwable cause = exception.getCause();
    List<String> causeMessages = new ArrayList<>();

    while (cause != null && cause != cause.getCause()) {
      String causeMessage = cause.getMessage();
      if (causeMessage != null
          && !causeMessage.isEmpty()
          && !causeMessage.equals(topLevelMessage)
          && !causeMessages.contains(causeMessage)) {
        causeMessages.add(causeMessage);
      }
      cause = cause.getCause();
    }

    if (!causeMessages.isEmpty()) {
      if (message.length() > 0) {
        message.append(". ");
      }
      message.append("Root cause: ");
      message.append(String.join(". ", causeMessages));
    }

    return message.length() > 0 ? message.toString() : DEFAULT_ERROR_MESSAGE;
  }
}
