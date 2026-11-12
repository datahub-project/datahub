package com.linkedin.metadata.search.embedding;

import javax.annotation.Nonnull;

/** Preserves whether an embedding failure is safe to retry at the consumer boundary. */
public class EmbeddingProviderException extends RuntimeException {

  private final boolean retryable;
  private final boolean systemic;

  public EmbeddingProviderException(@Nonnull String message, boolean retryable) {
    this(message, retryable, false);
  }

  public EmbeddingProviderException(@Nonnull String message, boolean retryable, boolean systemic) {
    super(message);
    this.retryable = retryable;
    this.systemic = systemic;
  }

  public EmbeddingProviderException(
      @Nonnull String message, @Nonnull Throwable cause, boolean retryable) {
    this(message, cause, retryable, false);
  }

  public EmbeddingProviderException(
      @Nonnull String message, @Nonnull Throwable cause, boolean retryable, boolean systemic) {
    super(message, cause);
    this.retryable = retryable;
    this.systemic = systemic;
  }

  public boolean isRetryable() {
    return retryable;
  }

  /**
   * A systemic failure (rejected credential, unknown model, vector dimension mismatch) recurs for
   * every document, so a batch consumer should abort on it rather than skipping one item.
   * Item-scoped failures (an oversized or malformed single input) are not systemic and are skipped
   * per URN. Distinct from {@link #isRetryable()}, which governs whether the failing call itself is
   * worth retrying: retryable failures are transient and never systemic.
   */
  public boolean isSystemic() {
    return systemic;
  }

  /**
   * Classifies a provider HTTP status as systemic. 401/403 reject the credential and 404 rejects
   * the configured model, so every URN in a backfill hits the same wall. 400 is deliberately
   * excluded: providers use it for document-specific validation failures such as an oversized
   * input, which must stay skippable per URN rather than aborting the run.
   */
  public static boolean isSystemicHttpStatus(final int status) {
    return status == 401 || status == 403 || status == 404;
  }
}
