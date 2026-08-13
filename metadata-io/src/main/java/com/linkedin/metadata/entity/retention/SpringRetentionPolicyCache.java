package com.linkedin.metadata.entity.retention;

import static com.datahub.util.RecordUtils.toJsonString;
import static com.datahub.util.RecordUtils.toRecordTemplate;

import com.linkedin.retention.Retention;
import java.time.Clock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;

/**
 * {@link RetentionPolicyCache} over a Spring {@link Cache}. The named cache is Caffeine or
 * Hazelcast depending on {@code searchService.cacheImplementation}. Default TTL matches {@code
 * cache.primary.ttlSeconds} (10 minutes). App-level TTL is checked on read as a backstop.
 */
@Slf4j
public class SpringRetentionPolicyCache implements RetentionPolicyCache {

  private final Cache cache;
  private final long ttlMillis;
  private final Clock clock;

  public SpringRetentionPolicyCache(@Nonnull Cache cache, long ttlSeconds) {
    this(cache, ttlSeconds, Clock.systemUTC());
  }

  public SpringRetentionPolicyCache(@Nonnull Cache cache, long ttlSeconds, @Nonnull Clock clock) {
    this.cache = cache;
    this.ttlMillis = ttlSeconds * 1000L;
    this.clock = clock;
  }

  @Override
  @Nullable
  public Retention get(@Nonnull String entityName, @Nonnull String aspectName) {
    try {
      CachedRetention cached = cache.get(cacheKey(entityName, aspectName), CachedRetention.class);
      if (cached == null) {
        return null;
      }
      if (ttlMillis > 0 && clock.millis() - cached.getCachedAtMillis() > ttlMillis) {
        cache.evict(cacheKey(entityName, aspectName));
        return null;
      }
      return toRecordTemplate(Retention.class, cached.getRetentionJson());
    } catch (Exception e) {
      log.warn(
          "Failed to read retention policy cache for entity={} aspect={}",
          entityName,
          aspectName,
          e);
      return null;
    }
  }

  @Override
  public void put(
      @Nonnull String entityName, @Nonnull String aspectName, @Nonnull Retention retention) {
    try {
      cache.put(
          cacheKey(entityName, aspectName),
          new CachedRetention(toJsonString(retention), clock.millis()));
    } catch (Exception e) {
      log.warn(
          "Failed to write retention policy cache for entity={} aspect={}",
          entityName,
          aspectName,
          e);
    }
  }

  @Override
  public void invalidateAll() {
    try {
      cache.invalidate();
    } catch (Exception e) {
      log.warn("Failed to invalidate retention policy cache", e);
    }
  }

  static String cacheKey(@Nonnull String entityName, @Nonnull String aspectName) {
    return entityName + '\0' + aspectName;
  }
}
