package io.datahubproject.metadata.context.graphql;

/** Shared GraphQL HTTP entry-point constants. */
public final class GraphqlHttpConstants {

  /** Resolver key when the HTTP request has no GraphQL operation name. */
  public static final String ANONYMOUS_OPERATION_NAME = "graphql";

  private GraphqlHttpConstants() {}
}
