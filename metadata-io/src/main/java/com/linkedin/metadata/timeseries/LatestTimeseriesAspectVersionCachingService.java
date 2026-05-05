package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig.CacheConfig;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
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

  private static final String CACHE_NAME = "latestTimeseriesAspect";

  private final TimeseriesAspectService delegate;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final Set<String> cachedAspectNames;
  private final ObjectMapper objectMapper;

  public LatestTimeseriesAspectVersionCachingService(
      @Nonnull final TimeseriesAspectService delegate,
      @Nonnull final CacheManager cacheManager,
      @Nonnull final CacheConfig cacheConfig,
      @Nonnull final ObjectMapper objectMapper,
      @Nonnull final Set<String> cachedAspectNames) {
    this.delegate = delegate;
    this.cache = cacheManager.getCache(CACHE_NAME);
    this.cacheConfig = cacheConfig;
    this.objectMapper = objectMapper;
    this.cachedAspectNames = cachedAspectNames;
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
      String cacheKey = buildCacheKey(entityName, aspectName, urn.toString());
      String cached = getCachedValue(cacheKey);
      if (cached != null) {
        try {
          List<EnvelopedAspect> result = deserializeCachedAspect(cached);
          log.debug("Cache hit for {}", cacheKey);
          return result;
        } catch (Exception e) {
          log.warn("Failed to deserialize cached aspect for {}, falling back to ES", cacheKey, e);
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
      String cacheKey = buildCacheKey(entityName, aspectName, urn.toString());
      putCachedValue(cacheKey, result.get(0));
    }

    return result;
  }

  @Override
  public Map<Urn, Map<String, EnvelopedAspect>> getLatestTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      @Nullable Map<String, Long> endTimeMillis) {
    return delegate.getLatestTimeseriesAspectValues(opContext, urns, aspectNames, endTimeMillis);
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
      evictCacheForAspect();
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
    String result =
        delegate.deleteAspectValuesAsync(opContext, entityName, aspectName, filter, options);
    if (cachedAspectNames.contains(aspectName)) {
      evictCacheForAspect();
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
    String result = delegate.reindexAsync(opContext, entityName, aspectName, filter, options);
    if (cachedAspectNames.contains(aspectName)) {
      evictCacheForAspect();
    }
    return result;
  }

  @Override
  public DeleteAspectValuesResult rollbackTimeseriesAspects(
      @Nonnull OperationContext opContext, @Nonnull final String runId) {
    DeleteAspectValuesResult result = delegate.rollbackTimeseriesAspects(opContext, runId);
    if (cache != null) {
      cache.clear();
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
        if (urn != null && cache != null) {
          EnvelopedAspect aspect =
              objectMapper.readValue(document.toString(), EnvelopedAspect.class);
          String cacheKey = buildCacheKey(entityName, aspectName, urn);
          putCachedValue(cacheKey, aspect);
        }
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
    return (limit == null || limit == 1)
        && startTimeMillis == null
        && endTimeMillis == null
        && (filter == null || (filter.getCriteria() != null && filter.getCriteria().isEmpty()))
        && sort == null;
  }

  private String buildCacheKey(String entityName, String aspectName, String urn) {
    return "latest:" + entityName + ":" + aspectName + ":" + urn;
  }

  private void evictCacheForAspect() {
    if (cache == null) {
      return;
    }
    try {
      cache.clear();
      log.debug("Cleared latest aspect version cache");
    } catch (Exception e) {
      log.warn("Failed to clear cache", e);
    }
  }

  @Nullable
  private String getCachedValue(String cacheKey) {
    if (cache == null) {
      return null;
    }
    try {
      Cache.ValueWrapper wrapper = cache.get(cacheKey);
      if (wrapper != null) {
        return (String) wrapper.get();
      }
      return null;
    } catch (Exception e) {
      log.warn("Cache get failed for {}, falling back to ES", cacheKey, e);
      return null;
    }
  }

  private void putCachedValue(String cacheKey, EnvelopedAspect aspect) {
    if (cache == null) {
      return;
    }
    try {
      String serialized = serializeAspect(aspect);
      long baseTtlSecs = (long) cacheConfig.getTtlHours() * 3600;
      int jitterMinutes = cacheConfig.getTtlJitterMinutes();
      int jitterRangeSecs = jitterMinutes * 60;
      int jitterSecs = ThreadLocalRandom.current().nextInt(2 * jitterRangeSecs) - jitterRangeSecs;
      long effectiveTtlSecs = Math.max(60, baseTtlSecs + jitterSecs);

      try {
        Object nativeCache = cache.getNativeCache();
        if (nativeCache instanceof com.hazelcast.map.IMap) {
          com.hazelcast.map.IMap<String, String> imap =
              (com.hazelcast.map.IMap<String, String>) nativeCache;
          imap.put(cacheKey, serialized, effectiveTtlSecs, TimeUnit.SECONDS);
        } else {
          cache.put(cacheKey, serialized);
        }
      } catch (ClassCastException | UnsupportedOperationException e) {
        cache.put(cacheKey, serialized);
      }
      log.debug("Cached {} with TTL={}s", cacheKey, effectiveTtlSecs);
    } catch (Exception e) {
      log.warn("Cache put failed for {}", cacheKey, e);
    }
  }

  private String serializeAspect(EnvelopedAspect aspect) throws Exception {
    return objectMapper.writeValueAsString(aspect);
  }

  private List<EnvelopedAspect> deserializeCachedAspect(String cached) throws Exception {
    EnvelopedAspect aspect = objectMapper.readValue(cached, EnvelopedAspect.class);
    return List.of(aspect);
  }
}
