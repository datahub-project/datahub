package com.datahub.graphql;

import org.springframework.http.converter.HttpMessageNotWritableException;

/** Signals a GraphQL response serialization failure before the HTTP response was committed. */
public class GraphQLResponseSerializationException extends HttpMessageNotWritableException {

  public GraphQLResponseSerializationException(Throwable cause) {
    super("Failed to serialize GraphQL response", cause);
  }
}
