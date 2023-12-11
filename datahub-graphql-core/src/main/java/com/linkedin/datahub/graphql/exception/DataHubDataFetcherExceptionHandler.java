package com.linkedin.datahub.graphql.exception;

import graphql.PublicApi;
import graphql.execution.DataFetcherExceptionHandler;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import lombok.extern.slf4j.Slf4j;

@PublicApi
@Slf4j
public class DataHubDataFetcherExceptionHandler implements DataFetcherExceptionHandler {

  @Override
  public DataFetcherExceptionHandlerResult onException(
      DataFetcherExceptionHandlerParameters handlerParameters) {
    Throwable exception = handlerParameters.getException();
    SourceLocation sourceLocation = handlerParameters.getSourceLocation();
    ResultPath path = handlerParameters.getPath();

    log.error("Failed to execute DataFetcher", exception);

    DataHubGraphQLErrorCode errorCode = DataHubGraphQLErrorCode.SERVER_ERROR;
    String message = "An unknown error occurred.";

    // note: make sure to access the true error message via `getCause()`
    if (exception.getCause() instanceof IllegalArgumentException) {
      errorCode = DataHubGraphQLErrorCode.BAD_REQUEST;
      message = exception.getCause().getMessage();
    }

    if (exception instanceof DataHubGraphQLException) {
      errorCode = ((DataHubGraphQLException) exception).errorCode();
      message = exception.getMessage();
    }

    if (exception.getCause() instanceof DataHubGraphQLException) {
      errorCode = ((DataHubGraphQLException) exception.getCause()).errorCode();
      message = exception.getCause().getMessage();
    }

    DataHubGraphQLError error = new DataHubGraphQLError(message, path, sourceLocation, errorCode);
    return DataFetcherExceptionHandlerResult.newResult().error(error).build();
  }
}
