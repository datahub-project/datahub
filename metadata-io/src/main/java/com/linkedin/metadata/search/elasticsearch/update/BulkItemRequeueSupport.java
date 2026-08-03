package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.graph.elastic.GraphEdgeWriteVersionFence;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;

/**
 * Bounded same-processor requeue of failed bulk items. Attempt keys are {@code index:id}. Callers
 * must map {@link Outcome} distinctly: {@link Outcome#DECLINED_STALE} is intentional drop of a
 * superseded op (complete pending, never unrecovered); {@link Outcome#EXHAUSTED} / {@link
 * Outcome#DISABLED} are terminal failures.
 */
@Slf4j
public class BulkItemRequeueSupport {
  private final boolean enabled;
  @Getter private final int maxAttempts;
  private final ConcurrentMap<String, Integer> attemptsByKey = new ConcurrentHashMap<>();
  private final Consumer<DocWriteRequest<?>> requeueCallback;

  public enum Outcome {
    /** Request was requeued; item remains pending. */
    REQUEUED,
    /** Newer version for the same docId already submitted; treat as completed, not unrecovered. */
    DECLINED_STALE,
    /** Max requeue attempts exceeded. */
    EXHAUSTED,
    /** Requeue disabled or request was null. */
    DISABLED
  }

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
   * Attempt to requeue a failed bulk item.
   *
   * @return outcome for the caller to map to tracker / metrics (never treat {@link
   *     Outcome#DECLINED_STALE} as unrecovered transfer failure)
   */
  @Nonnull
  public Outcome tryRequeue(@Nullable DocWriteRequest<?> request) {
    if (!isEnabled() || request == null) {
      return Outcome.DISABLED;
    }
    if (GraphEdgeWriteVersionFence.INSTANCE.shouldDeclineRequeue(request)) {
      attemptsByKey.remove(attemptKey(request));
      return Outcome.DECLINED_STALE;
    }
    String key = attemptKey(request);
    int attempt = attemptsByKey.merge(key, 1, Integer::sum);
    if (attempt > maxAttempts) {
      attemptsByKey.remove(key);
      GraphEdgeWriteVersionFence.INSTANCE.clearRequest(request);
      log.warn(
          "Bulk item requeue exhausted for index [{}] id [{}] after {} attempts",
          request.index(),
          request.id(),
          maxAttempts);
      return Outcome.EXHAUSTED;
    }
    log.debug(
        "Requeueing bulk item index [{}] id [{}] attempt {}/{}",
        request.index(),
        request.id(),
        attempt,
        maxAttempts);
    requeueCallback.accept(request);
    return Outcome.REQUEUED;
  }

  public void clearAttempts(@Nullable DocWriteRequest<?> request) {
    if (request != null) {
      attemptsByKey.remove(attemptKey(request));
      GraphEdgeWriteVersionFence.INSTANCE.clearRequest(request);
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
