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

    IllegalArgumentException illException =
        findFirstThrowableCauseOfClass(exception, IllegalArgumentException.class);
    if (illException != null) {
      log.error("Illegal Argument Exception");
      errorCode = DataHubGraphQLErrorCode.BAD_REQUEST;
      message = illException.getMessage();
    }

    DataHubGraphQLException graphQLException =
        findFirstThrowableCauseOfClass(exception, DataHubGraphQLException.class);
    if (graphQLException != null) {
      errorCode = graphQLException.errorCode();
      message = graphQLException.getMessage();
    }

    DataHubGraphQLError error = new DataHubGraphQLError(message, path, sourceLocation, errorCode);
    return DataFetcherExceptionHandlerResult.newResult().error(error).build();
  }

  <T extends Throwable> T findFirstThrowableCauseOfClass(Throwable throwable, Class<T> clazz) {
    while (throwable != null && throwable.getCause() != null) {
      if (throwable.getCause().getClass().equals(clazz)) {
        return (T) throwable.getCause();
      } else {
        throwable = throwable.getCause();
      }
    }
    return null;
  }
}
