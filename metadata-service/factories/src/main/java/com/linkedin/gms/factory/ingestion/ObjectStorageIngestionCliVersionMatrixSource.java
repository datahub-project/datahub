package com.linkedin.gms.factory.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.BaseServiceException;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrix;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixParser;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixSource;
import com.linkedin.metadata.ingestion.MatrixRefreshFailure;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import jakarta.annotation.PreDestroy;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * {@link IngestionCliVersionMatrixSource} backed by any {@link ObjectStorageClient} — S3, GCS, or
 * the local filesystem — selected by the scheme of {@code ingestion.cliVersionMatrix.uri}. Cloud
 * reads use GMS's ambient credentials, so a private bucket needs no separate secret distributed to
 * GMS.
 *
 * <p>Wired in the factory module rather than alongside the no-op source in {@code configuration}
 * because that is where the storage clients and their credential resolution already live, keeping
 * the lightweight {@code configuration} module free of AWS/GCP dependencies.
 *
 * <p>Runtime contract: background refresh on a fixed interval, atomic lock-free cache swap read by
 * {@link #getMatrix()}, and last-known-good retention on fetch or parse failure so in-flight
 * executions never see a flapping view. Parsing and validation are delegated to the shared {@link
 * IngestionCliVersionMatrixParser}, so every backend enforces one schema.
 */
@Slf4j
public class ObjectStorageIngestionCliVersionMatrixSource
    implements IngestionCliVersionMatrixSource {

  /** Thread name for the background refresh worker; stands out in thread dumps. */
  private static final String REFRESH_THREAD_NAME = "ingestion-cli-version-matrix-refresh";

  /** Seconds to wait for the refresh thread to drain on graceful shutdown. */
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

  /** Bound on the cause-chain walk in {@link #classify}, so a cyclic chain cannot spin. */
  private static final int MAX_CAUSE_DEPTH = 10;

  private final ObjectStorageClient client;
  private final String objectKey;

  /** The configured URI, carried purely so log lines name the location an operator can go fix. */
  private final String displayUri;

  private final AtomicReference<IngestionCliVersionMatrix> cached;
  private final AtomicLong lastFetchedAtMillis;
  private final ObjectMapper objectMapper;

  /** Background refresh scheduler, stopped by {@link #shutdown()} on Spring context teardown. */
  private final ScheduledExecutorService executor;

  public ObjectStorageIngestionCliVersionMatrixSource(
      final ObjectStorageClient client,
      final String objectKey,
      final String displayUri,
      final int refreshIntervalSeconds) {
    this.client = client;
    this.objectKey = objectKey;
    this.displayUri = displayUri;
    this.cached = new AtomicReference<>(IngestionCliVersionMatrix.EMPTY);
    this.lastFetchedAtMillis = new AtomicLong(0L);
    this.objectMapper = new ObjectMapper();

    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, REFRESH_THREAD_NAME);
              // Daemon so the JVM can still exit cleanly if @PreDestroy somehow doesn't fire.
              t.setDaemon(true);
              return t;
            });
    // Fetch immediately on startup (delay=0), then repeat on the configured interval.
    this.executor.scheduleAtFixedRate(this::refresh, 0, refreshIntervalSeconds, TimeUnit.SECONDS);
  }

  /** Gracefully stop the background refresh on Spring context teardown. */
  @PreDestroy
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
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
      final String body = client.getObjectAsString(objectKey);
      final JsonNode root;
      try {
        root = objectMapper.readTree(body);
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
        // File-level schema violation. Refuse to swap the cache; the last-known-good matrix keeps
        // serving resolutions while the operator gets a fix-this-now WARN.
        logPayloadRejection(schemaError.getMessage());
        return;
      }
      cached.set(parsed);
      // Stamp the timestamp after the swap so readers never see a fresh stamp with a stale matrix.
      lastFetchedAtMillis.set(System.currentTimeMillis());
      log.info(
          "Successfully refreshed ingestion version matrix from {}; {} server version entries loaded",
          displayUri,
          parsed.size());
    } catch (Throwable t) {
      // Catch Throwable, not Exception: scheduleAtFixedRate silently cancels all future runs if a
      // task lets anything escape, so a single bad tick — including an OutOfMemoryError or
      // StackOverflowError from a huge or deeply-nested object — must not permanently freeze the
      // background refresh. Re-assert the interrupt flag if we were interrupted mid-fetch (e.g.
      // shutdownNow during context teardown) since the outer swallow would otherwise drop it.
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      MatrixRefreshFailure failure = classify(t);
      log.warn(
          "[{}] Failed to refresh ingestion version matrix from {}. Retaining last known matrix; {}.",
          failure.token(),
          displayUri,
          failure.hint(),
          t);
    }
  }

  private void logPayloadRejection(String detail) {
    log.warn(
        "[{}] Refusing to swap matrix cache from {}: {}. Retaining last known matrix; {}.",
        MatrixRefreshFailure.PAYLOAD.token(),
        displayUri,
        detail,
        MatrixRefreshFailure.PAYLOAD.hint());
  }

  /**
   * Maps a storage failure onto an operator-actionable class. The clients wrap provider exceptions
   * in a {@link RuntimeException}, so the cause chain is walked rather than the top-level type. AWS
   * and GCS both expose an HTTP-shaped status code, which is what distinguishes "denied" from
   * "missing"; the local backend surfaces the equivalent as {@code java.nio.file} exceptions.
   *
   * <p>Package-private for direct unit testing — classification drives what an operator is told to
   * fix, so it is behaviour worth asserting rather than an implementation detail.
   */
  static MatrixRefreshFailure classify(Throwable t) {
    // Bounded walk: a self-referential or cyclic cause chain must not spin here.
    Throwable cause = t;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof JsonProcessingException) {
        return MatrixRefreshFailure.PAYLOAD;
      }
      if (cause instanceof AccessDeniedException) {
        return MatrixRefreshFailure.PERMISSION;
      }
      if (cause instanceof NoSuchFileException) {
        return MatrixRefreshFailure.NOT_FOUND;
      }
      if (cause instanceof SdkServiceException awsError) {
        return MatrixRefreshFailure.forHttpStatus(awsError.statusCode());
      }
      if (cause instanceof BaseServiceException gcpError) {
        return MatrixRefreshFailure.forHttpStatus(gcpError.getCode());
      }
      cause = cause.getCause();
    }
    return MatrixRefreshFailure.TRANSPORT;
  }
}
