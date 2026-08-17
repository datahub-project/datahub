package com.linkedin.metadata.kafka.hook;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
public class HookUtils {

  // TODO: Don't need this, just use from EntityKeyUtils
  /**
   * Extracts and returns an {@link Urn} from a {@link MetadataChangeLog}. Extracts from either an
   * entityUrn or entityKey field, depending on which is present.
   */
  public static Urn getUrnFromEvent(
      @Nonnull final MetadataChangeLog event, @Nonnull final EntityRegistry entityRegistry) {
    EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException(
          "Failed to get urn from MetadataChangeLog event. Skipping processing.", e);
    }
    // Extract an URN from the Log Event.
    return EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());
  }

  /**
   * Joins a hook's offloaded future, unwrapping the {@link CompletionException} that {@link
   * CompletableFuture#join()} would otherwise wrap around a failure. This keeps the offloaded path
   * exception-equivalent to running the work inline: the original {@link RuntimeException} (or
   * {@link Error}) propagates out of the hook unchanged, so the listener's existing at-most-once
   * handling treats it exactly as it did before the work was offloaded.
   */
  public static <T> T unwrapJoin(@Nonnull final CompletableFuture<T> future) {
    try {
      return future.join();
    } catch (CompletionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
  }

  /**
   * Waits for every offloaded future to complete, then — like {@link #unwrapJoin} — rethrows the
   * <b>first</b> failure unwrapped. All futures are always awaited, but if more than one task fails
   * <b>only the first exception propagates and the rest are silently swallowed</b> (this is {@link
   * CompletableFuture#allOf}'s semantics). Use it only with tasks that either isolate their own
   * errors internally (e.g. the delete tasks, which log and never rethrow) or where surfacing a
   * single failure is sufficient — the hook path is at-most-once, so one propagated failure and N
   * propagated failures are handled identically by the listener (log + skip, no retry).
   */
  public static void awaitAll(@Nonnull final CompletableFuture<?>... futures) {
    unwrapJoin(CompletableFuture.allOf(futures));
  }

  /**
   * Submits an offloaded hook task on the given executor, carrying the caller's SLF4J {@link MDC}
   * context onto the worker thread and restoring the worker's prior context afterward. The Kafka
   * listener sets entity-urn/aspect/change-type into the MDC on the consumer thread; without this,
   * logs emitted from the offloaded task (which runs on a pool or virtual thread) would lose that
   * correlation context.
   */
  @Nonnull
  public static CompletableFuture<Void> runAsync(
      @Nonnull final Runnable task, @Nonnull final Executor executor) {
    final Map<String, String> callerContext = MDC.getCopyOfContextMap();
    return CompletableFuture.runAsync(
        () -> {
          final Map<String, String> workerContext = MDC.getCopyOfContextMap();
          if (callerContext != null) {
            MDC.setContextMap(callerContext);
          } else {
            MDC.clear();
          }
          try {
            task.run();
          } finally {
            if (workerContext != null) {
              MDC.setContextMap(workerContext);
            } else {
              MDC.clear();
            }
          }
        },
        executor);
  }

  private HookUtils() {}
}
