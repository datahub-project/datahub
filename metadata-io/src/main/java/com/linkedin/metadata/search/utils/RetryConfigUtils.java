package com.linkedin.metadata.search.utils;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.function.Predicate;
import org.opensearch.OpenSearchException;

public class RetryConfigUtils {

  /**
   * Predicate for transient failures that should be retried. Matches: - OpenSearchException
   * (cluster/index errors) - SocketTimeoutException (network timeouts) - IOException (I/O failures)
   * - RuntimeException (wrapping IOException from lambda contexts) And unwraps causes to handle
   * nested exceptions.
   */
  private static final Predicate<Throwable> EXCEPTION_PREDICATE =
      (e) ->
          (e instanceof OpenSearchException
              || e instanceof SocketTimeoutException
              || e instanceof IOException
              || e instanceof RuntimeException
              || (e.getCause() != null
                  && (e.getCause() instanceof OpenSearchException
                      || e.getCause() instanceof SocketTimeoutException
                      || e.getCause() instanceof IOException)));

  public static final RetryConfig EXPONENTIAL =
      RetryConfig.custom()
          .maxAttempts(3)
          .retryOnException(EXCEPTION_PREDICATE)
          .intervalFunction(IntervalFunction.ofExponentialBackoff(100, 2.0))
          .build();

  /**
   * Health check retry with exponential backoff. Retries on any Exception (transient failures like
   * network timeouts). Note: IOException may be wrapped in RuntimeException by call sites, so we
   * catch Exception to handle both. For checking cluster/index health status during reindex
   * finalization. 3 attempts with 100/200/400ms waits = ~700ms total.
   */
  public static final RetryConfig HEALTH_CHECK =
      RetryConfig.custom()
          .maxAttempts(3)
          .intervalFunction(IntervalFunction.ofExponentialBackoff(100, 2.0))
          .retryOnException(EXCEPTION_PREDICATE)
          .failAfterMaxAttempts(true)
          .build();

  /**
   * Cost estimation retry with exponential backoff. Retries on IOException and RuntimeException
   * (transient failures like network timeouts). IOException may be wrapped in RuntimeException by
   * call sites (lambdas can't throw checked exceptions). For estimating index costs during reindex
   * classification. 3 attempts with 1000/2000/4000ms waits = ~7 seconds total. Falls back to
   * sequential mode on failure.
   */
  public static final RetryConfig COST_ESTIMATION =
      RetryConfig.custom()
          .maxAttempts(3)
          .intervalFunction(IntervalFunction.ofExponentialBackoff(1000, 2.0))
          .retryOnException(EXCEPTION_PREDICATE)
          .failAfterMaxAttempts(true)
          .build();

  /**
   * Index settings update retry with exponential backoff. Retries on any Exception (transient
   * failures like network timeouts or circuit breaker exceptions). Note: IOException may be wrapped
   * in RuntimeException by call sites, so we catch Exception to handle both. For updating index
   * settings during finalization (replica restoration and settings restoration). 3 attempts with
   * 200/400/800ms waits = ~1.4 seconds total.
   */
  public static final RetryConfig SETTINGS_UPDATE =
      RetryConfig.custom()
          .maxAttempts(3)
          .intervalFunction(IntervalFunction.ofExponentialBackoff(300, 2.0))
          .retryOnException(EXCEPTION_PREDICATE)
          .failAfterMaxAttempts(true)
          .build();

  /**
   * Task status polling retry with exponential backoff. Retries on any Exception (transient
   * failures like network timeouts or circuit breaker exceptions during task monitoring). Note:
   * IOException may be wrapped in RuntimeException by call sites, so we catch Exception to handle
   * both. 3 attempts with 100/200/400ms waits = ~700ms total.
   */
  public static final RetryConfig TASK_STATUS =
      RetryConfig.custom()
          .maxAttempts(3)
          .intervalFunction(IntervalFunction.ofExponentialBackoff(300, 2.0))
          .retryOnException(EXCEPTION_PREDICATE)
          .failAfterMaxAttempts(true)
          .build();

  /**
   * Document count validation retry with exponential backoff. Retries on Exception (doc count
   * mismatch after reindex due to ES flush delays). 10 attempts with 2000/4000/8000/16000ms waits
   * to allow segment merging and index refresh.
   */
  public static RetryConfig constructDocValidationRetry(long intervalMs, int retryCount) {
    return RetryConfig.custom()
        .maxAttempts(retryCount)
        .intervalFunction(IntervalFunction.ofExponentialBackoff(intervalMs, 2))
        .retryOnException(EXCEPTION_PREDICATE)
        .failAfterMaxAttempts(true)
        .build();
  }

  public static final RetryConfig DOC_COUNT_REFRESH =
      RetryConfig.custom()
          .maxAttempts(3)
          .intervalFunction(IntervalFunction.ofExponentialBackoff(700, 3))
          .retryOnException(EXCEPTION_PREDICATE)
          .failAfterMaxAttempts(true)
          .build();
}
