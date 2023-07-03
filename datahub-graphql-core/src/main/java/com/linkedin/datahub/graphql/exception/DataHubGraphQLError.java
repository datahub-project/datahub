package com.linkedin.datahub.graphql.exception;

import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorHelper;
import graphql.PublicApi;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.*;


@PublicApi
public class DataHubGraphQLError implements GraphQLError {

  private final String message;
  private final List<Object> path;
  private final DataHubGraphQLErrorCode errorCode;
  private final List<SourceLocation> locations;
  private final Map<String, Object> extensions;

  public DataHubGraphQLError(String message, ResultPath path, SourceLocation sourceLocation, DataHubGraphQLErrorCode errorCode) {
    this.path = assertNotNull(path).toList();
    this.errorCode = assertNotNull(errorCode);
    this.locations = Collections.singletonList(sourceLocation);
    this.message = message;
    this.extensions = buildExtensions(errorCode);
  }

  private Map<String, Object> buildExtensions(DataHubGraphQLErrorCode errorCode) {
    final Map<String, Object> extensions = new LinkedHashMap<>();
    extensions.put("code", errorCode.getCode());
    extensions.put("type", errorCode.toString());
    return extensions;
  }

  public int getErrorCode() {
    return errorCode.getCode();
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public List<SourceLocation> getLocations() {
    return locations;
  }

  @Override
  public List<Object> getPath() {
    return path;
  }

  @Override
  public Map<String, Object> getExtensions() {
    return extensions;
  }

  @Override
  public ErrorType getErrorType() {
    return ErrorType.DataFetchingException;
  }

  @Override
  public String toString() {
    return "DataHubGraphQLError{"
        + "path="
        + path
        + ", code="
        + errorCode
        + ", locations="
        + locations
        + '}';
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return GraphqlErrorHelper.equals(this, o);
  }

  @Override
  public int hashCode() {
    return GraphqlErrorHelper.hashCode(this);
  }
}

