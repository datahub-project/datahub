package com.linkedin.metadata.graph.elastic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;

/**
 * JVM-local fence for graph edge bulk ops: tracks the latest submitted {@code graphWriteVersion}
 * per edge docId and the version on each in-flight {@link DocWriteRequest}. Used to decline stale
 * bulk item requeues (older upsert after newer delete, or older delete after newer upsert).
 *
 * <p>Requeue runs in the same process that submitted the op, so an in-process fence is sufficient
 * for that path. Search index writes are unaffected (they never call {@link #recordSubmit}).
 */
@Slf4j
public final class GraphEdgeWriteVersionFence {
  public static final GraphEdgeWriteVersionFence INSTANCE = new GraphEdgeWriteVersionFence();

  private static final int MAX_DOC_IDS = 1_000_000;

  private final Cache<String, Long> latestByDocId =
      Caffeine.newBuilder().maximumSize(MAX_DOC_IDS).expireAfterAccess(1, TimeUnit.HOURS).build();

  /** Identity map: same request object is requeued by {@code BulkItemRequeueSupport}. */
  private final ConcurrentHashMap<DocWriteRequest<?>, Long> requestVersions =
      new ConcurrentHashMap<>();

  private GraphEdgeWriteVersionFence() {}

  /**
   * Record a graph edge write before it is added to the bulk processor. No-op when version is null
   * (legacy / unversioned callers).
   */
  public void recordSubmit(
      @Nonnull String docId, @Nullable Long version, @Nonnull DocWriteRequest<?> request) {
    if (version == null) {
      return;
    }
    latestByDocId.asMap().merge(docId, version, Long::max);
    requestVersions.put(request, version);
  }

  /**
   * @return true if this request should not be requeued because a newer version for the same docId
   *     was already submitted
   */
  public boolean shouldDeclineRequeue(@Nullable DocWriteRequest<?> request) {
    if (request == null || request.id() == null) {
      return false;
    }
    Long opVersion = requestVersions.get(request);
    if (opVersion == null) {
      return false;
    }
    Long latest = latestByDocId.getIfPresent(request.id());
    if (latest != null && opVersion < latest) {
      log.debug(
          "Declining stale graph bulk requeue for docId [{}] opVersion {} latest {}",
          request.id(),
          opVersion,
          latest);
      requestVersions.remove(request);
      return true;
    }
    return false;
  }

  public void clearRequest(@Nullable DocWriteRequest<?> request) {
    if (request != null) {
      requestVersions.remove(request);
    }
  }

  /** Test-only: drop all fence state. */
  public void resetForTesting() {
    synchronized (GraphEdgeWriteVersionFence.class) {
      latestByDocId.invalidateAll();
      requestVersions.clear();
    }
  }
}
