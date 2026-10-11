package com.linkedin.metadata.search.elasticsearch.update;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;

/**
 * Bounded same-processor requeue of failed bulk items. Attempt keys are {@code index:id}. When
 * requeue is declined (disabled or exhausted), the caller records LWW exhaustion or unrecovered
 * transfer failure.
 */
@Slf4j
public class BulkItemRequeueSupport {
  private final boolean enabled;
  @Getter private final int maxAttempts;
  private final ConcurrentMap<String, Integer> attemptsByKey = new ConcurrentHashMap<>();
  private final Consumer<DocWriteRequest<?>> requeueCallback;

  public BulkItemRequeueSupport(
      boolean enabled, int maxAttempts, @Nonnull Consumer<DocWriteRequest<?>> requeueCallback) {
    this.enabled = enabled;
    this.maxAttempts = Math.max(0, maxAttempts);
    this.requeueCallback = requeueCallback;
  }

  public boolean isEnabled() {
    return enabled && maxAttempts > 0;
  }

  /**
   * @return true if the request was requeued (still pending); false if it should be treated as
   *     terminal for this failure
   */
  public boolean tryRequeue(@Nullable DocWriteRequest<?> request) {
    if (!isEnabled() || request == null) {
      return false;
    }
    String key = attemptKey(request);
    int attempt = attemptsByKey.merge(key, 1, Integer::sum);
    if (attempt > maxAttempts) {
      attemptsByKey.remove(key);
      log.warn(
          "Bulk item requeue exhausted for index [{}] id [{}] after {} attempts",
          request.index(),
          request.id(),
          maxAttempts);
      return false;
    }
    log.debug(
        "Requeueing bulk item index [{}] id [{}] attempt {}/{}",
        request.index(),
        request.id(),
        attempt,
        maxAttempts);
    requeueCallback.accept(request);
    return true;
  }

  public void clearAttempts(@Nullable DocWriteRequest<?> request) {
    if (request != null) {
      attemptsByKey.remove(attemptKey(request));
    }
  }

  public void clearAttempts(@Nullable String index, @Nullable String id) {
    if (index != null || id != null) {
      attemptsByKey.remove(attemptKey(index, id));
    }
  }

  static String attemptKey(@Nonnull DocWriteRequest<?> request) {
    return attemptKey(request.index(), request.id());
  }

  static String attemptKey(@Nullable String index, @Nullable String id) {
    return String.valueOf(index) + ":" + String.valueOf(id);
  }
}
