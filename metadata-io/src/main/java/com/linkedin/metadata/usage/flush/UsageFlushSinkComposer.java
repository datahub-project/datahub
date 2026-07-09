package com.linkedin.metadata.usage.flush;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Delegates flush batches to all registered {@link UsageFlushSink} beans.
 *
 * <p>Tracks per-batch publish progress so store-level retries skip delegates that already
 * succeeded. Without this, a partial failure (e.g. a downstream sink down after Micrometer export)
 * double-count non-idempotent sinks such as Micrometer counters.
 */
@Slf4j
@RequiredArgsConstructor
public class UsageFlushSinkComposer implements UsageFlushSink {

  private final List<UsageFlushSink> delegates;

  /** Batch instance → delegates that already published successfully for that batch. */
  private final Map<UsageFlushBatch, Set<UsageFlushSink>> succeededDelegates =
      Collections.synchronizedMap(new HashMap<>());

  @Override
  public void publish(@Nonnull UsageFlushBatch batch) {
    Set<UsageFlushSink> succeeded =
        succeededDelegates.computeIfAbsent(batch, ignored -> new HashSet<>());
    RuntimeException lastFailure = null;
    for (UsageFlushSink sink : delegates) {
      if (sink == this) {
        continue;
      }
      if (succeeded.contains(sink)) {
        continue;
      }
      try {
        sink.publish(batch);
        succeeded.add(sink);
      } catch (RuntimeException e) {
        log.warn("Usage flush sink {} failed", sink.getClass().getSimpleName(), e);
        lastFailure = e;
      }
    }
    if (lastFailure != null) {
      throw lastFailure;
    }
    succeededDelegates.remove(batch);
  }
}
