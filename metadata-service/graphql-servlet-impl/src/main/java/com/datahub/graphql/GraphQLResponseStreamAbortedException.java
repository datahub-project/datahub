package com.datahub.graphql;

import org.springframework.http.converter.HttpMessageNotWritableException;

/**
 * Signals a GraphQL streaming write failed after the HTTP response was committed (client abort or
 * mid-stream IO). Status and body cannot be replaced; the converter has already logged and counted.
 */
public class GraphQLResponseStreamAbortedException extends HttpMessageNotWritableException {

  public GraphQLResponseStreamAbortedException(Throwable cause) {
    super("GraphQL response stream aborted after commit", cause);
  }
}
