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
public class CustomDataFetcherExceptionHandler implements DataFetcherExceptionHandler {

  @Override
  public DataFetcherExceptionHandlerResult onException(DataFetcherExceptionHandlerParameters handlerParameters) {
    Throwable exception = handlerParameters.getException();
    SourceLocation sourceLocation = handlerParameters.getSourceLocation();
    ResultPath path = handlerParameters.getPath();

    log.error("Failed to execute DataFetcher", exception);

    CustomGraphQLErrorCode errorCode = CustomGraphQLErrorCode.SERVER_ERROR;

    if (exception instanceof CustomGraphQLException) {
      errorCode = ((CustomGraphQLException) exception).errorCode();
    }

    if (exception.getCause() instanceof CustomGraphQLException) {
      errorCode = ((CustomGraphQLException) exception.getCause()).errorCode();
    }

    CustomGraphQLError error = new CustomGraphQLError(exception.getMessage(), path, sourceLocation, errorCode);
    return DataFetcherExceptionHandlerResult.newResult().error(error).build();
  }
}
