package com.linkedin.metadata.timeseries;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.map.IMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig.CacheConfig;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

@Slf4j
public class LatestTimeseriesAspectVersionCachingService
    implements TimeseriesAspectService, ElasticSearchIndexed {

  /** Cache holding {@code latest:<entity>:<aspect>:<urn>} → {@link CachedLatestAspect}. */
  private static final String DATA_CACHE_NAME = "latestTimeseriesAspect";

  /**
   * Cache holding {@code aspect-index:<entity>:<aspect>} → {@code Set<String>} of cached URNs. Used
   * to evict all data keys for an aspect on delete / async-delete / reindex / rollback.
   */
  private static final String INDEX_CACHE_NAME = "latestTimeseriesAspectIndex";

  private final TimeseriesAspectService delegate;
  private final Cache dataCache;
  private final Cache indexCache;
  private final CacheConfig cacheConfig;
  private final Set<String> cachedAspectNames;

  public LatestTimeseriesAspectVersionCachingService(
      @Nonnull final TimeseriesAspectService delegate,
      @Nonnull final CacheManager cacheManager,
      @Nonnull final CacheConfig cacheConfig) {
    this.delegate = delegate;
    this.dataCache = cacheManager.getCache(DATA_CACHE_NAME);
    this.indexCache = cacheManager.getCache(INDEX_CACHE_NAME);
    if (this.dataCache == null || this.indexCache == null) {
      throw new IllegalArgumentException(
          "Both '"
              + DATA_CACHE_NAME
              + "' and '"
              + INDEX_CACHE_NAME
              + "' caches must be registered with the CacheManager");
    }
    this.cacheConfig = cacheConfig;
    // CacheConfig is the single source of truth for which aspects are cached. Snapshot once
    // so we don't see config mutations mid-request.
    this.cachedAspectNames = Set.copyOf(cacheConfig.getCachedAspects());
  }

  @Override
  public void configure() {
    delegate.configure();
  }

  @Override
  public long countByFilter(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter) {
    return delegate.countByFilter(opContext, entityName, aspectName, filter);
  }

  @Override
  public List<EnvelopedAspect> getAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter) {
    return getAspectValuesImpl(
        opContext,
        urn,
        entityName,
        aspectName,
        startTimeMillis,
        endTimeMillis,
        limit,
        filter,
        null);
  }

  @Override
  public List<EnvelopedAspect> getAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort) {
    return getAspectValuesImpl(
        opContext,
        urn,
        entityName,
        aspectName,
        startTimeMillis,
        endTimeMillis,
        limit,
        filter,
        sort);
  }

  private List<EnvelopedAspect> getAspectValuesImpl(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort) {
    if (isLatestValueQuery(limit, startTimeMillis, endTimeMillis, filter, sort)
        && cachedAspectNames.contains(aspectName)) {
      String cacheKey = buildCacheKey(aspectName, entityName, urn.toString());
      String cached = getCachedValue(cacheKey);
      if (cached != null) {
        try {
          List<EnvelopedAspect> result = deserializeCachedAspect(cached);
          log.debug("Cache hit for {}", cacheKey);
          return result;
        } catch (Exception e) {
          log.error("Failed to deserialize cached aspect for {}, falling back to ES", cacheKey, e);
        }
      }
    }

    List<EnvelopedAspect> result =
        delegate.getAspectValues(
            opContext,
            urn,
            entityName,
            aspectName,
            startTimeMillis,
            endTimeMillis,
            limit,
            filter,
            sort);

    if (isLatestValueQuery(limit, startTimeMillis, endTimeMillis, filter, sort)
        && cachedAspectNames.contains(aspectName)
        && !result.isEmpty()) {
      EnvelopedAspect latest = result.get(0);
      long ts = extractTimestampMillisFromAspect(latest, opContext);
      putCachedValue(aspectName, entityName, urn.toString(), latest, ts);
    }

    return result;
  }

  @Override
  public Map<Urn, Map<String, EnvelopedAspect>> getLatestTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      @Nullable Map<String, Long> endTimeMillis) {
    // endTimeMillis turns this into a point-in-time query, which the latest-only cache can't
    // serve. Bypass entirely.
    if (endTimeMillis != null) {
      return delegate.getLatestTimeseriesAspectValues(opContext, urns, aspectNames, endTimeMillis);
    }

    // Per-URN split: which aspects came from cache (hits), which still need to be fetched
    // from the delegate (missing). "Missing" includes both cache misses on cacheable aspects
    // AND aspects that aren't configured to be cached at all — both have to round-trip to ES.
    Map<Urn, Map<String, EnvelopedAspect>> result = new HashMap<>();
    Map<Urn, Set<String>> stillNeeded = new HashMap<>();

    for (Urn urn : urns) {
      Map<String, EnvelopedAspect> hits = new HashMap<>();
      Set<String> missing = new HashSet<>();
      for (String aspectName : aspectNames) {
        if (!cachedAspectNames.contains(aspectName)) {
          missing.add(aspectName);
          continue;
        }
        String cacheKey = buildCacheKey(aspectName, urn.getEntityType(), urn.toString());
        String cachedValue = getCachedValue(cacheKey);
        if (cachedValue == null) {
          missing.add(aspectName);
          continue;
        }
        try {
          hits.put(aspectName, deserializeCachedAspect(cachedValue).get(0));
        } catch (Exception e) {
          log.warn("Cache deserialization failed for {}, refetching from ES", cacheKey, e);
          missing.add(aspectName);
        }
      }
      if (!hits.isEmpty()) {
        result.put(urn, hits);
      }
      if (!missing.isEmpty()) {
        stillNeeded.put(urn, missing);
      }
    }

    if (stillNeeded.isEmpty()) {
      return result;
    }

    // The delegate API takes a single aspectNames set, not per-URN. Build the union of what's
    // actually missing across the URNs that need a round-trip; we filter back to per-URN needs
    // before merging so a cache hit on one URN isn't accidentally overwritten by an ES result
    // for a different URN that needed the same aspect.
    Set<String> aspectsToFetch = new HashSet<>();
    for (Set<String> perUrnMissing : stillNeeded.values()) {
      aspectsToFetch.addAll(perUrnMissing);
    }

    Map<Urn, Map<String, EnvelopedAspect>> dbResults =
        delegate.getLatestTimeseriesAspectValues(
            opContext, stillNeeded.keySet(), aspectsToFetch, endTimeMillis);

    dbResults.forEach(
        (urn, aspectMap) -> {
          Set<String> neededForThisUrn = stillNeeded.get(urn);
          if (neededForThisUrn == null) {
            return;
          }
          aspectMap.forEach(
              (aspectName, aspect) -> {
                // Only merge what this URN actually needed — the union-of-aspects fetch may
                // have returned aspects we already had cached for this URN. Don't overwrite
                // those merged hits.
                if (!neededForThisUrn.contains(aspectName)) {
                  return;
                }
                result.computeIfAbsent(urn, k -> new HashMap<>()).put(aspectName, aspect);
                if (cachedAspectNames.contains(aspectName)) {
                  try {
                    long ts = extractTimestampMillisFromAspect(aspect, opContext);
                    putCachedValue(aspectName, urn.getEntityType(), urn.toString(), aspect, ts);
                  } catch (Exception e) {
                    log.error("Failed to cache urn {} aspect {}", urn, aspectName, e);
                  }
                }
              });
        });

    return result;
  }

  @Override
  public GenericTable getAggregatedStats(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {
    return delegate.getAggregatedStats(
        opContext, entityName, aspectName, aggregationSpecs, filter, groupingBuckets);
  }

  @Override
  public DeleteAspectValuesResult deleteAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter) {
    DeleteAspectValuesResult result =
        delegate.deleteAspectValues(opContext, entityName, aspectName, filter);
    if (cachedAspectNames.contains(aspectName)) {
      evictAspectIndex(entityName, aspectName);
    }
    return result;
  }

  @Override
  public String deleteAspectValuesAsync(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    // Eviction policy: we evict the cache *immediately* when the async job is kicked off,
    // before ES has finished deleting. Two consequences worth understanding:
    //
    //   1. Concurrent reads during the async window will miss the cache and read stale data
    //      from ES (the data the async job is on the way to deleting). The cache then
    //      repopulates with that stale snapshot. When the async job completes, the cache may
    //      hold entries ES has since dropped, and they live until TTL expiry or the next
    //      upsert overwrites them.
    //
    //   2. The alternative — evict on async completion — would mean serving stale cached
    //      data for the entire async window, with no signal that a delete was even in
    //      progress. We pick fresh-fetch-and-temporarily-stale-refill over guaranteed-stale
    //      because the failure mode is more visible (cache and ES disagree briefly) and
    //      self-heals on TTL.
    //
    // Acceptable for a best-effort latest-aspect cache. If we ever need strict "no stale
    // after delete starts," wire a completion callback or a periodic re-evict.
    String result =
        delegate.deleteAspectValuesAsync(opContext, entityName, aspectName, filter, options);
    if (cachedAspectNames.contains(aspectName)) {
      evictAspectIndex(entityName, aspectName);
    }
    return result;
  }

  @Override
  public String reindexAsync(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    // Same eviction policy as deleteAspectValuesAsync: evict now, accept that concurrent
    // reads during the async reindex window may temporarily refill the cache from a stale
    // ES snapshot. Self-heals via TTL or the next upsert. See the comment on
    // deleteAspectValuesAsync for the full rationale.
    String result = delegate.reindexAsync(opContext, entityName, aspectName, filter, options);
    if (cachedAspectNames.contains(aspectName)) {
      evictAspectIndex(entityName, aspectName);
    }
    return result;
  }

  @Override
  public DeleteAspectValuesResult rollbackTimeseriesAspects(
      @Nonnull OperationContext opContext, @Nonnull final String runId) {
    DeleteAspectValuesResult result = delegate.rollbackTimeseriesAspects(opContext, runId);
    if (cache != null) {
      evictCacheForAllAspects();
    }
    return result;
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {
    delegate.upsertDocument(opContext, entityName, aspectName, docId, document);
    if (cachedAspectNames.contains(aspectName)) {
      String urn = null;
      try {
        urn = document.has("urn") ? document.get("urn").asText() : null;
        if (urn == null) {
          return;
        }

        // The cache write requires an event payload to wrap. Missing or null events are
        // anomalous (the transformer always populates the field for valid timeseries
        // aspects) — skip the cache write rather than caching an empty wrapper that
        // would mask the underlying problem.
        JsonNode eventNode = document.get("event");
        if (eventNode == null || eventNode.isNull()) {
          log.debug(
              "upsertDocument: missing event node for {}/{}/{}, skipping cache write",
              entityName,
              aspectName,
              urn);
          return;
        }

        long ts = extractTimestampMillisFromDocument(document);
        if (ts == 0L) {
          // Without a comparable timestamp, put-if-newer can't make a sound decision —
          // a wrapper with ts=0 would be beaten by any subsequent older event with a
          // valid timestamp, leaving stale data cached. Evict any existing entry instead
          // and let the next read repopulate from ES.
          log.warn(
              "upsertDocument: missing timestampMillis for {}/{}/{}, evicting cache key",
              entityName,
              aspectName,
              urn);
          evictCacheKey(aspectName, entityName, urn);
          return;
        }

        // EnvelopedAspect construction lives in ElasticSearchTimeseriesAspectService —
        // share it so cache writes can't drift from what ES reads return.
        EnvelopedAspect aspect =
            ElasticSearchTimeseriesAspectService.buildEnvelopedAspect(
                opContext, eventNode, document.get("systemMetadata"));
        putCachedValue(aspectName, entityName, urn, aspect, ts);
      } catch (Exception e) {
        log.warn("Failed to cache upserted aspect for {}", urn, e);
      }
    }
  }

  @Override
  public TimeseriesScrollResult scrollAspects(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return delegate.scrollAspects(
        opContext,
        entityName,
        aspectName,
        filter,
        sortCriteria,
        scrollId,
        count,
        startTimeMillis,
        endTimeMillis);
  }

  @Override
  public List<TimeseriesIndexSizeResult> getIndexSizes(@Nonnull OperationContext opContext) {
    return delegate.getIndexSizes(opContext);
  }

  @Override
  public Map<Urn, Map<String, Map<String, Object>>> raw(
      @Nonnull OperationContext opContext, @Nonnull Map<String, Set<String>> urnAspects) {
    return delegate.raw(opContext, urnAspects);
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> properties)
      throws IOException {
    if (delegate instanceof ElasticSearchIndexed) {
      return ((ElasticSearchIndexed) delegate).buildReindexConfigs(opContext, properties);
    }
    return List.of();
  }

  @Override
  public void reindexAll(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> properties)
      throws IOException {
    if (delegate instanceof ElasticSearchIndexed) {
      ((ElasticSearchIndexed) delegate).reindexAll(opContext, properties);
      evictCacheForAllAspects();
    }
  }

  @Override
  public ESIndexBuilder getIndexBuilder() {
    if (delegate instanceof ElasticSearchIndexed) {
      return ((ElasticSearchIndexed) delegate).getIndexBuilder();
    }
    return null;
  }

  private boolean isLatestValueQuery(
      @Nullable Integer limit,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Filter filter,
      @Nullable SortCriterion sort) {
    return limit != null
        && limit == 1
        && startTimeMillis == null
        && endTimeMillis == null
        && (filter == null || (filter.getCriteria() != null && filter.getCriteria().isEmpty()))
        && sort == null;
  }

  private void evictCacheForAllAspects() {
    try {
      dataCache.clear();
      indexCache.clear();
      log.debug("Cleared both timeseries caches (data + index)");
    } catch (Exception e) {
      log.warn("Failed to clear timeseries caches", e);
    }
  }

  /**
   * Single-key eviction. Used by {@code upsertDocument} when it can't determine the incoming
   * event's timestamp — evicting forces the next read to repopulate from ES rather than leaving a
   * possibly-stale wrapper in place.
   */
  private void evictCacheKey(String aspectName, String entityName, String urn) {
    try {
      dataCache.evict(buildCacheKey(aspectName, entityName, urn));
    } catch (Exception e) {
      log.warn("Failed to evict cache key for {}/{}/{}", aspectName, entityName, urn, e);
    }
  }

  @Nullable
  private String getCachedValue(String cacheKey) {
    try {
      Cache.ValueWrapper wrapper = dataCache.get(cacheKey);
      if (wrapper == null) {
        return null;
      }
      Object value = wrapper.get();
      if (value instanceof CachedLatestAspect cached) {
        return cached.getSerializedAspect();
      }
      // Any other shape at a data key indicates a cache-layer bug. Treat as miss; the
      // caller will fall through to ES.
      return null;
    } catch (Exception e) {
      log.warn("Cache get failed for {}, falling back to ES", cacheKey, e);
      return null;
    }
  }

  /**
   * Caches the given aspect under the URN-keyed slot, but only if the supplied event timestamp is
   * greater than or equal to the timestamp already cached. The compare-and-set is atomic on each
   * backend (Hazelcast: server-side {@link PutIfNewerProcessor}; Caffeine: {@code
   * asMap().compute(...)} on the underlying ConcurrentHashMap).
   *
   * <p>This protects against two race classes:
   *
   * <ul>
   *   <li><b>Out-of-order upserts</b> — a backfill or replay that emits an older event must not
   *       clobber a newer cached value.
   *   <li><b>Cache-fill vs. concurrent upsert</b> — a slow ES read that returns an older snapshot
   *       must not overwrite a newer wrapper written by an in-flight upsert.
   * </ul>
   */
  private void putCachedValue(
      String aspectName,
      String entityType,
      String urn,
      EnvelopedAspect aspect,
      long timestampMillis) {
    if (timestampMillis == 0L) {
      // Defensive: the upsert path explicitly evicts on ts=0, but cache-fill paths might
      // also hit this (a malformed aspect from ES with no extractable timestamp). Writing
      // a wrapper(ts=0) is unsafe — older legitimate writes would beat it and clobber the
      // ES-derived value. Skip the write instead.
      log.debug(
          "Skipping cache put for {}:{}:{} — extracted timestampMillis is 0",
          aspectName,
          entityType,
          urn);
      return;
    }
    String cacheKey = buildCacheKey(aspectName, entityType, urn);
    try {
      String serialized = serializeAspect(aspect);
      CachedLatestAspect wrapper = new CachedLatestAspect(timestampMillis, serialized);
      // Per-entry TTL with jitter is only honored on backends that support it (Hazelcast IMap).
      // Caffeine applies its single configured expireAfterWrite policy at the cache-build level,
      // so the jitter computed here is a no-op on that path. Single-node Caffeine deployments
      // therefore see uniform expiry; Hazelcast deployments get the stampede protection.
      long baseTtlSecs = (long) cacheConfig.getTtlHours() * 3600;
      int jitterMinutes = cacheConfig.getTtlJitterMinutes();
      int jitterRangeSecs = jitterMinutes * 60;
      int jitterSecs =
          jitterRangeSecs == 0
              ? 0
              : ThreadLocalRandom.current().nextInt(2 * jitterRangeSecs + 1) - jitterRangeSecs;
      long effectiveTtlSecs = Math.max(60, baseTtlSecs + jitterSecs);

      try {
        boolean accepted = putIfNewerNative(cacheKey, wrapper, effectiveTtlSecs);
        if (accepted) {
          addUrnToReverseIndex(aspectName, entityType, urn, effectiveTtlSecs);
          log.debug("Cached {} (ts={}) with TTL={}s", cacheKey, timestampMillis, effectiveTtlSecs);
        } else {
          log.debug(
              "Skipped cache write for {} — incoming ts={} not newer than cached",
              cacheKey,
              timestampMillis);
        }
      } catch (ClassCastException | UnsupportedOperationException e) {
        // Unknown native cache backend — fall back to the Spring abstraction. We lose the
        // atomic compare-and-set guarantee on this path, but it's bounded to test/dev setups
        // (ConcurrentMapCacheManager etc.) where concurrency is not the primary concern.
        log.warn(
            "Native cache put failed for {}, falling back to unconditional Spring cache.put",
            cacheKey,
            e);
        dataCache.put(cacheKey, wrapper);
      }
    } catch (Exception e) {
      log.warn("Cache put failed for {}", cacheKey, e);
    }
  }

  /**
   * Atomic put-if-newer for the supported native backends. Returns {@code true} if the wrapper was
   * accepted (written), {@code false} if rejected because an existing entry was newer.
   */
  @SuppressWarnings("unchecked")
  private boolean putIfNewerNative(String cacheKey, CachedLatestAspect incoming, long ttlSeconds) {
    Object nativeCache = dataCache.getNativeCache();

    if (nativeCache instanceof IMap<?, ?>) {
      // Server-side compare-and-set on the partition owner.
      //
      // KNOWN LIMITATION (non-atomic with setTtl): the EntryProcessor API doesn't carry a TTL
      // contract, so we run setTtl as a separate RPC after a successful write. If the GMS pod
      // dies between executeOnKey and setTtl, the entry is written but inherits the IMap's
      // default TTL (or no TTL, if the map has none configured) instead of our jittered
      // per-entry value. The next write to that key goes through this same path and re-applies
      // the TTL, so the leak is bounded to the dead-pod window — not a permanent leak.
      //
      // The "atomic" alternative is IMap.set(key, value, ttl, unit), but that loses the
      // compare-and-set semantics that PutIfNewerProcessor provides; a hacky workaround
      // (re-implementing the timestamp comparison via Hazelcast locks or a CAS on a separate
      // key) isn't worth the complexity. If this becomes a real problem, file a follow-up.
      IMap<String, CachedLatestAspect> hazelMap = (IMap<String, CachedLatestAspect>) nativeCache;
      Boolean accepted = hazelMap.executeOnKey(cacheKey, new PutIfNewerProcessor(incoming));
      if (Boolean.TRUE.equals(accepted)) {
        hazelMap.setTtl(cacheKey, ttlSeconds, TimeUnit.SECONDS);
        return true;
      }
      return false;
    }

    if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache<?, ?>) {
      // Caffeine doesn't support per-entry TTL via put(); entries inherit the cache's
      // configured expireAfterWrite (if any). The atomic compare-and-set comes from
      // ConcurrentHashMap.compute(), which holds the per-bin lock for the duration.
      com.github.benmanes.caffeine.cache.Cache<String, CachedLatestAspect> caffeineCache =
          (com.github.benmanes.caffeine.cache.Cache<String, CachedLatestAspect>) nativeCache;
      boolean[] accepted = new boolean[] {false};
      caffeineCache
          .asMap()
          .compute(
              cacheKey,
              (key, existing) -> {
                if (existing == null) {
                  accepted[0] = true;
                  return incoming;
                }
                if (incoming.getTimestampMillis() >= existing.getTimestampMillis()) {
                  accepted[0] = true;
                  return incoming;
                }
                return existing;
              });
      return accepted[0];
    }

    // Unknown native backend — surface to caller for fallback handling.
    throw new UnsupportedOperationException(
        "Unsupported native cache type: " + nativeCache.getClass().getName());
  }

  private String serializeAspect(EnvelopedAspect aspect) throws Exception {
    return RecordUtils.toJsonString(aspect);
  }

  /**
   * Top-level {@code timestampMillis} on the indexed document built by {@code
   * TimeseriesAspectTransformer}. Returns 0 if missing or unparseable; callers treat 0 as "older
   * than anything legitimate," so the corresponding cache entry will lose to any future write that
   * does carry a real timestamp.
   */
  private long extractTimestampMillisFromDocument(@Nonnull JsonNode document) {
    JsonNode tsNode = document.get("timestampMillis");
    if (tsNode == null || tsNode.isNull() || !tsNode.canConvertToLong()) {
      return 0L;
    }
    return tsNode.asLong();
  }

  /**
   * {@code timestampMillis} from inside an {@link EnvelopedAspect}'s serialized event payload.
   * Every timeseries aspect schema declares this field, so it should always be present on results
   * coming from ES. One Jackson tree parse per cache miss; cache hits skip this.
   */
  private long extractTimestampMillisFromAspect(
      @Nonnull EnvelopedAspect aspect, @Nonnull OperationContext opContext) {
    try {
      if (aspect.getAspect() == null || aspect.getAspect().getValue() == null) {
        return 0L;
      }
      String eventJson = aspect.getAspect().getValue().asString("UTF-8");
      JsonNode tsNode = opContext.getObjectMapper().readTree(eventJson).get("timestampMillis");
      if (tsNode == null || tsNode.isNull() || !tsNode.canConvertToLong()) {
        return 0L;
      }
      return tsNode.asLong();
    } catch (Exception e) {
      log.debug("Could not extract timestampMillis from aspect; defaulting to 0", e);
      return 0L;
    }
  }

  private List<EnvelopedAspect> deserializeCachedAspect(String cached) throws Exception {
    EnvelopedAspect aspect = RecordUtils.toRecordTemplate(EnvelopedAspect.class, cached);
    return List.of(aspect);
  }

  @SuppressWarnings("unchecked")
  private void addUrnToReverseIndex(
      String aspectName, String entityName, String urn, long ttlSeconds) {
    try {
      Object nativeIndex = indexCache.getNativeCache();
      if (nativeIndex instanceof com.github.benmanes.caffeine.cache.Cache) {
        // Caffeine reverse-index entries inherit the cache's default expiration policy and
        // can't be given a per-entry TTL — see putIfNewerNative. The index will still be
        // bounded by the cache's maxSize and explicit evictAspectIndex() calls on
        // delete/reindex/rollback paths.
        com.github.benmanes.caffeine.cache.Cache<String, Set<String>> caffeineIndex =
            (com.github.benmanes.caffeine.cache.Cache<String, Set<String>>) nativeIndex;
        handleAtomicAddToReverseIndex(caffeineIndex, aspectName, entityName, urn);
      } else if (nativeIndex instanceof IMap) {
        IMap<String, Set<String>> hazelIndex = (IMap<String, Set<String>>) nativeIndex;
        handleAtomicAddToReverseIndex(hazelIndex, aspectName, entityName, urn, ttlSeconds);
      }
    } catch (Exception e) {
      log.warn("Failed to update aspect index for {}", aspectName, e);
    }
  }

  /**
   * Bulk-evict every cached URN for an (entity, aspect) pair. The index entry lives in {@link
   * #indexCache} and the per-URN data entries live in {@link #dataCache} — the read-and-clear of
   * the index plus the per-URN invalidation on the data map are in two separate cache backends, so
   * we deliberately handle each side here.
   */
  @SuppressWarnings("unchecked")
  private void evictAspectIndex(String entityName, String aspectName) {
    try {
      Object nativeIndex = indexCache.getNativeCache();
      Object nativeData = dataCache.getNativeCache();
      if (nativeIndex instanceof com.github.benmanes.caffeine.cache.Cache
          && nativeData instanceof com.github.benmanes.caffeine.cache.Cache) {
        com.github.benmanes.caffeine.cache.Cache<String, Set<String>> caffeineIndex =
            (com.github.benmanes.caffeine.cache.Cache<String, Set<String>>) nativeIndex;
        com.github.benmanes.caffeine.cache.Cache<String, CachedLatestAspect> caffeineData =
            (com.github.benmanes.caffeine.cache.Cache<String, CachedLatestAspect>) nativeData;
        handleAtomicEviction(caffeineIndex, caffeineData, aspectName, entityName);
      } else if (nativeIndex instanceof IMap && nativeData instanceof IMap) {
        IMap<String, Set<String>> hazelIndex = (IMap<String, Set<String>>) nativeIndex;
        IMap<String, CachedLatestAspect> hazelData = (IMap<String, CachedLatestAspect>) nativeData;
        handleAtomicEviction(hazelIndex, hazelData, aspectName, entityName);
      }
    } catch (Exception e) {
      log.warn("Failed to evict aspect index for {}", aspectName, e);
    }
  }

  private String buildCacheKey(String aspect, String entity, String urn) {
    return new StringBuilder(
            "latest:".length() + entity.length() + aspect.length() + urn.length() + 3)
        .append("latest")
        .append(":")
        .append(entity)
        .append(':')
        .append(aspect)
        .append(':')
        .append(urn)
        .toString();
  }

  private String buildCacheKey(String aspect, String entityName) {
    return new StringBuilder("aspect-index:".length() + aspect.length() + entityName.length() + 2)
        .append("aspect-index")
        .append(":")
        .append(entityName)
        .append(':')
        .append(aspect)
        .toString();
  }

  private void handleAtomicEviction(
      com.github.benmanes.caffeine.cache.Cache<String, Set<String>> caffeineIndex,
      com.github.benmanes.caffeine.cache.Cache<String, CachedLatestAspect> caffeineData,
      String aspectName,
      String entityName) {
    // The index swap is atomic but per-URN invalidation on the data cache happens outside the
    // atomic block. A concurrent writer that adds a URN to the freshly-empty index between
    // the remove and the invalidate loop could leave its data key behind. This is a narrow
    // window in a single-node Caffeine deployment and is preferred over taking a global lock;
    // the data key will still expire via the cache's TTL/maxSize policy.
    String indexKey = buildCacheKey(aspectName, entityName);
    Set<String> urns = caffeineIndex.asMap().remove(indexKey);
    if (urns == null || urns.isEmpty()) {
      return;
    }
    for (String urn : urns) {
      caffeineData.invalidate(buildCacheKey(aspectName, entityName, urn));
    }
  }

  private void handleAtomicEviction(
      IMap<String, Set<String>> hazelIndex,
      IMap<String, CachedLatestAspect> hazelData,
      String aspectName,
      String entityName) {
    String indexKey = buildCacheKey(aspectName, entityName);
    Set<String> urns =
        hazelIndex.executeOnKey(indexKey, new GetAndClearUrnsFromReverseIndexProcessor());
    if (urns == null || urns.isEmpty()) {
      return;
    }
    for (String urn : urns) {
      hazelData.remove(buildCacheKey(aspectName, entityName, urn));
    }
  }

  private void handleAtomicAddToReverseIndex(
      com.github.benmanes.caffeine.cache.Cache<String, Set<String>> caffeineIndex,
      String aspectName,
      String entityName,
      String urn) {
    String indexKey = buildCacheKey(aspectName, entityName);
    caffeineIndex
        .asMap()
        .compute(
            indexKey,
            (key, existing) -> {
              Set<String> urns = existing == null ? new HashSet<>() : new HashSet<>(existing);
              urns.add(urn);
              return urns;
            });
  }

  private void handleAtomicAddToReverseIndex(
      IMap<String, Set<String>> hazelIndex,
      String aspectName,
      String entityName,
      String urn,
      long ttlSeconds) {
    // Same non-atomic-with-TTL caveat as putIfNewerNative: executeOnKey + setTtl are two
    // separate RPCs. A pod death between them leaves the index entry with the IMap's default
    // TTL rather than our intended one. Bounded leak — the next URN added to this index
    // re-applies the TTL.
    String indexKey = buildCacheKey(aspectName, entityName);
    hazelIndex.executeOnKey(indexKey, new AddUrnToReverseIndexProcessor(urn));
    hazelIndex.setTtl(indexKey, ttlSeconds, TimeUnit.SECONDS);
  }
}
