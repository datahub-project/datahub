package com.linkedin.metadata.utils.retry;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

/** Shared exponential-backoff retry for flush and rollup publish paths. */
public final class ExponentialBackoffRetry {

  private ExponentialBackoffRetry() {}

  /**
   * Runs {@code action} with exponential backoff. Returns {@code true} when the action completes
   * without throwing; {@code false} when all attempts are exhausted.
   */
  public static boolean run(
      int maxAttempts,
      long initialBackoffMillis,
      @Nonnull CheckedRunnable action,
      @Nonnull BiConsumer<Integer, Throwable> onAttemptFailure) {
    int attempts = Math.max(1, maxAttempts);
    RetryConfig.Builder<?> builder = RetryConfig.custom().maxAttempts(attempts);
    if (initialBackoffMillis > 0) {
      builder.intervalFunction(IntervalFunction.ofExponentialBackoff(initialBackoffMillis, 2.0));
    }
    RetryConfig config = builder.retryOnException(t -> true).build();
    Retry retry = Retry.of("exponential-backoff", config);
    retry
        .getEventPublisher()
        .onRetry(
            event ->
                onAttemptFailure.accept(event.getNumberOfRetryAttempts(), event.getLastThrowable()))
        .onError(event -> onAttemptFailure.accept(attempts, event.getLastThrowable()));

    try {
      Retry.decorateCheckedRunnable(retry, toVavr(action)).run();
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  @FunctionalInterface
  public interface CheckedRunnable {
    void run() throws Exception;
  }

  private static io.vavr.CheckedRunnable toVavr(@Nonnull CheckedRunnable action) {
    return () -> action.run();
  }
}
