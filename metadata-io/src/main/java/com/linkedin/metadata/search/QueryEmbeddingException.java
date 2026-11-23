package com.linkedin.metadata.search;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A vector rung could not embed its query, so no neighbor was retrieved. {@link #getReason()} is
 * one of the codes below, named as keywordRerank names its reasons where one exists. This build
 * raises {@link #TIMEOUT} and {@link #FAILED}. {@link #REJECTED}, {@link #RESERVE_EXHAUSTED}, and
 * {@link #BUDGET_EXHAUSTED} are for an embedding service that throttles or budgets query
 * embeddings, which throws this exception with that reason itself.
 */
public class QueryEmbeddingException extends RuntimeException {
  /** The embedding did not finish before the search's deadline. */
  public static final String TIMEOUT = "timeout";

  /** The embedding service throttled the request. */
  public static final String REJECTED = "rejected";

  /** The reserve of query embeddings is exhausted. */
  public static final String RESERVE_EXHAUSTED = "reserveExhausted";

  /** The query budget is exhausted. */
  public static final String BUDGET_EXHAUSTED = "budgetExhausted";

  /** Any other failure of the embedding provider. */
  public static final String FAILED = "failed";

  private final String reason;

  public QueryEmbeddingException(@Nonnull String reason, @Nullable Throwable cause) {
    super("Semantic search could not embed the query: " + reason, cause);
    this.reason = reason;
  }

  @Nonnull
  public String getReason() {
    return reason;
  }
}
