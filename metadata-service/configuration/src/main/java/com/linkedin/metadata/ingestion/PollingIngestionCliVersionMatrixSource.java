package com.linkedin.metadata.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IngestionCliVersionMatrixSource} that polls a {@link MatrixDocumentReader} on a fixed
 * interval and serves the last successfully parsed matrix. The reader decides where the document
 * comes from; everything below is identical for every backend, which is why it lives here once
 * rather than per-store.
 *
 * <p>The document must follow this schema:
 *
 * <pre>{@code
 * {
 *   "1.3.1.4": {
 *     "snowflake": {
 *       "_default": "1.3.1.4",
 *       "cohorts": [
 *         { "version": "1.3.1.5", "deployments": ["deployment-1", "deployment-2"] }
 *       ]
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Runtime contract: background refresh on a fixed interval, atomic lock-free cache swap read by
 * {@link #getMatrix()} (a single volatile read on {@link AtomicReference#get()}), and
 * last-known-good retention on read or parse failure so in-flight executions never see a flapping
 * view. Parsing and validation are delegated to {@link IngestionCliVersionMatrixParser}, so every
 * backend enforces one schema.
 */
@Slf4j
public class PollingIngestionCliVersionMatrixSource implements IngestionCliVersionMatrixSource {

  /**
   * Thread name for the background refresh worker. Named so it stands out in thread dumps (the
   * default {@code Executors.defaultThreadFactory()} would produce {@code pool-N-thread-1}, which
   * gives an operator triaging a hung pod no idea what the thread does).
   */
  private static final String REFRESH_THREAD_NAME = "ingestion-cli-version-matrix-refresh";

  /** Seconds to wait for the refresh thread to drain on graceful shutdown. */
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

  /** Bound on the cause-chain walk in {@link #classify}, so a cyclic chain cannot spin. */
  private static final int MAX_CAUSE_DEPTH = 10;

  /** ObjectMapper is thread-safe once configured, so one instance serves every source. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final MatrixDocumentReader reader;
  private final AtomicReference<IngestionCliVersionMatrix> cached;
  private final AtomicLong lastFetchedAtMillis;

  /** Background refresh scheduler, stopped by {@link #shutdown()} on Spring context teardown. */
  private final ScheduledExecutorService executor;

  public PollingIngestionCliVersionMatrixSource(
      final MatrixDocumentReader reader, final int refreshIntervalSeconds) {
    this.reader = reader;
    this.cached = new AtomicReference<>(IngestionCliVersionMatrix.EMPTY);
    this.lastFetchedAtMillis = new AtomicLong(0L);

    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, REFRESH_THREAD_NAME);
              // Daemon so the JVM can still exit cleanly if @PreDestroy somehow doesn't fire
              // (kill -9, container-runtime quirks). PreDestroy remains the primary shutdown path.
              t.setDaemon(true);
              return t;
            });
    // Fetch immediately on startup (delay=0), then repeat on the configured interval.
    this.executor.scheduleAtFixedRate(this::refresh, 0, refreshIntervalSeconds, TimeUnit.SECONDS);
  }

  /** The location being polled — lets callers log or assert which backend is bound. */
  public String displayUri() {
    return reader.displayUri();
  }

  /**
   * Gracefully stop the background refresh on Spring context teardown. Spring invokes this hook
   * during bean destruction so the scheduled-executor thread does not leak across context restarts
   * (relevant in dev hot-reload and integration-test contexts that re-create the bean).
   */
  @PreDestroy
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        // Refresh in progress took longer than the timeout — interrupt it. If a read was mid-IO the
        // connection will close, and parseMatrix is purely in-memory so it can't dangle.
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public IngestionCliVersionMatrix getMatrix() {
    return cached.get();
  }

  @Override
  public long getLastFetchedAtMillis() {
    return lastFetchedAtMillis.get();
  }

  /** Package-private so tests can force a refresh without waiting for the scheduled tick. */
  void refresh() {
    try {
      final String body = reader.read();
      final JsonNode root;
      try {
        root = OBJECT_MAPPER.readTree(body);
      } catch (JsonProcessingException notJson) {
        // Read fine, but the bytes aren't JSON — a payload problem, not an access or network one.
        // Jackson's message carries the offending line/column, which is the actionable part.
        logPayloadRejection(notJson.getOriginalMessage());
        return;
      }
      final IngestionCliVersionMatrix parsed;
      try {
        parsed = IngestionCliVersionMatrixParser.parseMatrix(root);
      } catch (IllegalArgumentException schemaError) {
        // File-level schema violation (e.g. root not a JSON object). Refuse to swap the cache; the
        // last-known-good matrix keeps serving resolutions while the operator gets a fix-this-now
        // WARN. Per-entry violations are handled inside parseMatrix without throwing.
        logPayloadRejection(schemaError.getMessage());
        return;
      }
      cached.set(parsed);
      // Stamp the timestamp after the swap so readers never see a fresh stamp with a stale matrix.
      lastFetchedAtMillis.set(System.currentTimeMillis());
      log.info(
          "Successfully refreshed ingestion version matrix from {}; {} server version entries loaded",
          reader.displayUri(),
          parsed.size());
    } catch (Throwable t) {
      // Catch Throwable, not Exception: scheduleAtFixedRate silently cancels all future runs if a
      // task lets anything escape, so a single bad tick — including an OutOfMemoryError or
      // StackOverflowError from a huge or deeply-nested document — must not permanently freeze the
      // background refresh. Re-assert the interrupt flag if we were interrupted mid-read (e.g.
      // shutdownNow during context teardown) since the outer swallow would otherwise drop it.
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      MatrixRefreshFailure failure = classify(t);
      log.warn(
          "[{}] Failed to refresh ingestion version matrix from {}. Retaining last known matrix; {}.",
          failure.token(),
          reader.displayUri(),
          failure.hint(),
          t);
    }
  }

  private void logPayloadRejection(String detail) {
    log.warn(
        "[{}] Refusing to swap matrix cache from {}: {}. Retaining last known matrix; {}.",
        MatrixRefreshFailure.PAYLOAD.token(),
        reader.displayUri(),
        detail,
        MatrixRefreshFailure.PAYLOAD.hint());
  }

  /**
   * Readers classify what they can and wrap it in a {@link MatrixReadException}; the chain is
   * walked rather than the top-level type inspected because a backend client may re-wrap on the way
   * out. Anything unclassified is transport by definition — retried on the next tick rather than
   * pointing the operator at a fix they don't need to make.
   *
   * <p>Package-private for direct unit testing — classification drives what an operator is told to
   * fix, so it is behaviour worth asserting rather than an implementation detail.
   */
  static MatrixRefreshFailure classify(Throwable t) {
    // Bounded walk: a self-referential or cyclic cause chain must not spin here.
    Throwable cause = t;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof MatrixReadException classified) {
        return classified.failure();
      }
      cause = cause.getCause();
    }
    return MatrixRefreshFailure.TRANSPORT;
  }
}
