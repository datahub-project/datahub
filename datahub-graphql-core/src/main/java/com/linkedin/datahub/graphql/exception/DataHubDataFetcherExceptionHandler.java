package com.linkedin.datahub.graphql.exception;

import graphql.PublicApi;
import graphql.execution.DataFetcherExceptionHandler;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@PublicApi
@Slf4j
public class DataHubDataFetcherExceptionHandler implements DataFetcherExceptionHandler {

  private static final String DEFAULT_ERROR_MESSAGE = "An unknown error occurred.";

  @Override
  public CompletableFuture<DataFetcherExceptionHandlerResult> handleException(
      DataFetcherExceptionHandlerParameters handlerParameters) {
    Throwable exception = handlerParameters.getException();
    SourceLocation sourceLocation = handlerParameters.getSourceLocation();
    ResultPath path = handlerParameters.getPath();

    DataHubGraphQLErrorCode errorCode = DataHubGraphQLErrorCode.SERVER_ERROR;
    String message = DEFAULT_ERROR_MESSAGE;

    IllegalArgumentException illException =
        findFirstThrowableCauseOfClass(exception, IllegalArgumentException.class);
    if (illException != null) {
      log.error("Failed to execute", illException);
      errorCode = DataHubGraphQLErrorCode.BAD_REQUEST;
      message = extractErrorMessage(illException);
    }

    DataHubGraphQLException graphQLException =
        findFirstThrowableCauseOfClass(exception, DataHubGraphQLException.class);
    if (graphQLException != null) {
      log.error("Failed to execute", graphQLException);
      errorCode = graphQLException.errorCode();
      message = extractErrorMessage(graphQLException);
    }

    ValidationException validationException =
        findFirstThrowableCauseOfClass(exception, ValidationException.class);
    if (validationException != null) {
      log.error("Failed to execute", validationException);
      errorCode = DataHubGraphQLErrorCode.BAD_REQUEST;
      message = extractErrorMessage(validationException);
    }

    IllegalStateException illegalStateException =
        findFirstThrowableCauseOfClass(exception, IllegalStateException.class);
    if (message.equals(DEFAULT_ERROR_MESSAGE) && illegalStateException != null) {
      log.error("Failed to execute", illegalStateException);
      errorCode = DataHubGraphQLErrorCode.SERVER_ERROR;
      message = extractErrorMessage(illegalStateException);
    }

    RuntimeException runtimeException =
        findFirstThrowableCauseOfClass(exception, RuntimeException.class);
    if (message.equals(DEFAULT_ERROR_MESSAGE) && runtimeException != null) {
      log.error("Failed to execute", runtimeException);
      errorCode = DataHubGraphQLErrorCode.SERVER_ERROR;
      message = extractErrorMessage(runtimeException);
    }

    if (illException == null
        && graphQLException == null
        && validationException == null
        && illegalStateException == null
        && runtimeException == null) {
      log.error("Failed to execute", exception);
    }
    DataHubGraphQLError error = new DataHubGraphQLError(message, path, sourceLocation, errorCode);
    return CompletableFuture.completedFuture(
        DataFetcherExceptionHandlerResult.newResult().error(error).build());
  }

  <T extends Throwable> T findFirstThrowableCauseOfClass(Throwable throwable, Class<T> clazz) {
    while (throwable != null) {
      if (clazz.isInstance(throwable)) {
        return (T) throwable;
      } else {
        throwable = throwable.getCause();
      }
    }
    return null;
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

    // Start with the top-level message
    String topLevelMessage = exception.getMessage();
    if (topLevelMessage != null && !topLevelMessage.isEmpty()) {
      message.append(topLevelMessage);
    }

    // Walk the exception chain to find root causes
    Throwable cause = exception.getCause();
    java.util.List<String> causeMessages = new java.util.ArrayList<>();

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

    // Append root cause messages
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
