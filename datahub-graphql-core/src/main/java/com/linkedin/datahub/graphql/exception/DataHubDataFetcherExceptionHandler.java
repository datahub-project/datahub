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
      message = illException.getMessage();
    }

    DataHubGraphQLException graphQLException =
        findFirstThrowableCauseOfClass(exception, DataHubGraphQLException.class);
    if (graphQLException != null) {
      log.error("Failed to execute", graphQLException);
      errorCode = graphQLException.errorCode();
      message = graphQLException.getMessage();
    }

    if (illException == null && graphQLException == null) {
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
}
