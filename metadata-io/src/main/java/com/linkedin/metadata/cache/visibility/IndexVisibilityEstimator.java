package com.linkedin.metadata.cache.visibility;

import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.RefreshIntervalResolver;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Computes when a write submitted now will be searchable in a source index, from configuration
 * alone. A cache that rebuilds from that index can serve its current content until then and treat
 * it as superseded afterwards. Nothing here forces an index refresh.
 *
 * <p>Both inputs come from the effective search configuration: the bulk processor settings (flush
 * period, retries, item requeue, refresh policy) and the {@code refresh_interval} of the index. An
 * unset or non-positive refresh interval yields no bound, and callers then keep their drop-at-write
 * behavior.
 */
@Slf4j
public final class IndexVisibilityEstimator {

  /** Effective settings for the cluster that serves one index kind. */
  public record IndexWriteSettings(
      @Nullable RefreshIntervals refreshIntervals, @Nullable BulkProcessorConfiguration bulk) {}

  private static final IndexWriteSettings UNKNOWN = new IndexWriteSettings(null, null);

  /** An estimator that never produces a bound. */
  public static final IndexVisibilityEstimator DISABLED =
      new IndexVisibilityEstimator(kind -> UNKNOWN, IndexWriteFlusher.NONE);

  private final Function<SourceIndexKind, IndexWriteSettings> settingsByKind;
  private final IndexWriteFlusher flusher;
  private final Set<SourceIndexKind> warnedKinds = ConcurrentHashMap.newKeySet();

  public IndexVisibilityEstimator(
      @Nonnull Function<SourceIndexKind, IndexWriteSettings> settingsByKind,
      @Nonnull IndexWriteFlusher flusher) {
    this.settingsByKind = settingsByKind;
    this.flusher = flusher;
  }

  /** One invalidation pass. {@link VisibilityMode#FLUSH_AWAIT} flushes each kind at most once. */
  @Nonnull
  public Session newSession() {
    return new Session();
  }

  public final class Session {
    private final Map<SourceIndexKind, Boolean> flushOutcomes =
        new EnumMap<>(SourceIndexKind.class);

    private Session() {}

    /**
     * Epoch millis at which a write submitted now is searchable, including {@code marginSeconds}.
     *
     * @param entityName entity whose index holds the write, used for per-entity refresh overrides
     *     of {@link SourceIndexKind#ENTITY}; ignored for {@link SourceIndexKind#GRAPH}
     */
    @Nonnull
    public OptionalLong visibleAtMillis(
        @Nonnull SourceIndexKind kind,
        @Nullable String entityName,
        @Nonnull VisibilityMode mode,
        int marginSeconds) {
      IndexWriteSettings settings = settingsByKind.apply(kind);
      long refreshMillis = refreshMillis(kind, entityName, settings);
      if (refreshMillis < 0) {
        warnNoBoundOnce(kind);
        return OptionalLong.empty();
      }
      long submittedAtMillis = System.currentTimeMillis();
      long baseMillis = submittedAtMillis;
      long submitBoundMillis = bulkSubmitBoundMillis(settings.bulk());
      if (mode == VisibilityMode.FLUSH_AWAIT && flushOnce(kind)) {
        baseMillis = System.currentTimeMillis();
        submitBoundMillis = 0;
      }
      return OptionalLong.of(
          baseMillis + submitBoundMillis + refreshMillis + Math.max(0, marginSeconds) * 1000L);
    }

    private boolean flushOnce(SourceIndexKind kind) {
      return flushOutcomes.computeIfAbsent(
          kind,
          k -> {
            try {
              return flusher.flushAndAwait(k);
            } catch (RuntimeException e) {
              log.warn(
                  "Flush before cache visibility bound failed for {}; using submit bound", k, e);
              return false;
            }
          });
    }
  }

  /**
   * A graph that asks for deferral but gets no bound silently behaves as if it did not, which hides
   * a misconfigured refresh interval. Warn once per kind rather than on every write.
   */
  private void warnNoBoundOnce(SourceIndexKind kind) {
    if (warnedKinds.add(kind)) {
      log.warn(
          "No usable refresh interval for the {} index; entity graph caches that set"
              + " invalidation.visibility drop at write time instead. Set a positive refresh"
              + " interval for this index in elasticsearch.index.refreshIntervals.",
          kind);
    }
  }

  /** Millis until a refresh exposes an indexed write, or -1 when there is no usable interval. */
  private static long refreshMillis(
      SourceIndexKind kind, @Nullable String entityName, IndexWriteSettings settings) {
    if (isImmediateRefresh(settings.bulk())) {
      return 0L;
    }
    RefreshIntervals intervals = settings.refreshIntervals();
    if (intervals == null) {
      return -1L;
    }
    int seconds;
    try {
      seconds =
          kind == SourceIndexKind.GRAPH
              ? RefreshIntervalResolver.graphIndexSeconds(intervals)
              : RefreshIntervalResolver.entityIndexSeconds(intervals, entityName, "entity index");
    } catch (IllegalStateException e) {
      return -1L;
    }
    return seconds > 0 ? seconds * 1000L : -1L;
  }

  private static boolean isImmediateRefresh(@Nullable BulkProcessorConfiguration bulk) {
    if (bulk == null || bulk.getRefreshPolicy() == null) {
      return false;
    }
    String policy = bulk.getRefreshPolicy().trim().toUpperCase(Locale.ROOT);
    return "IMMEDIATE".equals(policy) || "WAIT_FOR".equals(policy);
  }

  /**
   * Worst case from enqueue to the bulk request being accepted: the flush period, plus the backoff
   * retries of that request, repeated for each bounded requeue of failed items.
   */
  private static long bulkSubmitBoundMillis(@Nullable BulkProcessorConfiguration bulk) {
    if (bulk == null) {
      return 0L;
    }
    long attemptSeconds =
        Math.max(0, bulk.getFlushPeriod())
            + (long) Math.max(0, bulk.getNumRetries()) * Math.max(0L, bulk.getRetryInterval());
    long passes =
        1L + (bulk.isItemRequeueEnabled() ? Math.max(0, bulk.getItemRequeueMaxAttempts()) : 0);
    return attemptSeconds * passes * 1000L;
  }
}
