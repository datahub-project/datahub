package com.linkedin.datahub.graphql.exception;

import com.linkedin.metadata.search.QueryEmbeddingException;
import javax.annotation.Nonnull;

/** The GraphQL error code for a query a search template's vector rung could not embed. */
public final class QueryEmbeddingErrorCodes {

  private QueryEmbeddingErrorCodes() {}

  /**
   * A query embedding that ran out of time is an exceeded deadline, and one the embedding service
   * throttled or budgeted out is a transient unavailability. Any other failure stays a server
   * error, since it may not clear on a retry.
   */
  @Nonnull
  public static DataHubGraphQLErrorCode of(@Nonnull final QueryEmbeddingException failure) {
    return switch (failure.getReason()) {
      case QueryEmbeddingException.TIMEOUT -> DataHubGraphQLErrorCode.DEADLINE_EXCEEDED;
      case QueryEmbeddingException.REJECTED,
          QueryEmbeddingException.RESERVE_EXHAUSTED,
          QueryEmbeddingException.BUDGET_EXHAUSTED ->
          DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE;
      default -> DataHubGraphQLErrorCode.SERVER_ERROR;
    };
  }
}
