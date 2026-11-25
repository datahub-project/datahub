package com.linkedin.metadata.search.embedding;

import javax.annotation.Nonnull;

/**
 * The embedding service throttled the request, and the provider's own retries, if it makes any, did
 * not get it through. Retryable later, and never systemic.
 */
public class EmbeddingThrottledException extends EmbeddingProviderException {

  public EmbeddingThrottledException(@Nonnull String message) {
    super(message, true);
  }

  public EmbeddingThrottledException(@Nonnull String message, @Nonnull Throwable cause) {
    super(message, cause, true);
  }
}
