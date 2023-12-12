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

  private static final String DEFAULT_ERROR_MESSAGE = "An unknown error occurred.";

  @Override
  public DataFetcherExceptionHandlerResult onException(
      DataFetcherExceptionHandlerParameters handlerParameters) {
    Throwable exception = handlerParameters.getException();
    SourceLocation sourceLocation = handlerParameters.getSourceLocation();
    ResultPath path = handlerParameters.getPath();

    log.error("Failed to execute DataFetcher", exception);

    DataHubGraphQLErrorCode errorCode = DataHubGraphQLErrorCode.SERVER_ERROR;
    String message = DEFAULT_ERROR_MESSAGE;

    IllegalArgumentException illException =
        findFirstThrowableCauseOfClass(exception, IllegalArgumentException.class);
    if (illException != null) {
      errorCode = DataHubGraphQLErrorCode.BAD_REQUEST;
      message = illException.getMessage();
    }

    DataHubGraphQLException graphQLException =
        findFirstThrowableCauseOfClass(exception, DataHubGraphQLException.class);
    if (graphQLException != null) {
      errorCode = graphQLException.errorCode();
      message = graphQLException.getMessage();
    }

    if (DEFAULT_ERROR_MESSAGE.equals(message)
        && exception.getCause() != null
        && exception.getCause() instanceof DataHubGraphQLException) {
      errorCode = ((DataHubGraphQLException) exception.getCause()).errorCode();
      message = exception.getCause().getMessage();
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
