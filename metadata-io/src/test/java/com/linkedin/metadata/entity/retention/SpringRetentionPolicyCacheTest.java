package com.linkedin.metadata.entity.retention;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.Test;

public class SpringRetentionPolicyCacheTest {

  @Test
  public void testPutGetRoundTrip() {
    SpringRetentionPolicyCache cache = newCache(Clock.systemUTC(), 300);
    Retention policy = new Retention().setVersion(new VersionBasedRetention().setMaxVersions(20));

    assertNull(cache.get("dataset", "schemaMetadata"));
    cache.put("dataset", "schemaMetadata", policy);

    Retention loaded = cache.get("dataset", "schemaMetadata");
    assertNotNull(loaded);
    assertEquals(loaded.getVersion().getMaxVersions(), 20);
  }

  @Test
  public void testTtlExpiryTreatedAsMiss() {
    Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    MutableClock clock = new MutableClock(t0);
    SpringRetentionPolicyCache cache = newCache(clock, 300);
    cache.put(
        "dataset",
        "schemaMetadata",
        new Retention().setVersion(new VersionBasedRetention().setMaxVersions(20)));

    assertNotNull(cache.get("dataset", "schemaMetadata"));

    clock.setInstant(t0.plusSeconds(301));
    assertNull(cache.get("dataset", "schemaMetadata"));
  }

  @Test
  public void testInvalidateAllClearsEntries() {
    SpringRetentionPolicyCache cache = newCache(Clock.systemUTC(), 300);
    cache.put(
        "dataset",
        "schemaMetadata",
        new Retention().setVersion(new VersionBasedRetention().setMaxVersions(20)));
    cache.invalidateAll();
    assertNull(cache.get("dataset", "schemaMetadata"));
  }

  @Test
  public void testEmptyRetentionIsCached() {
    SpringRetentionPolicyCache cache = newCache(Clock.systemUTC(), 300);
    cache.put("unknown", "aspect", new Retention());

    Retention loaded = cache.get("unknown", "aspect");
    assertNotNull(loaded);
    assertTrue(loaded.data().isEmpty());
  }

  private static SpringRetentionPolicyCache newCache(Clock clock, long ttlSeconds) {
    ConcurrentMapCacheManager cacheManager = new ConcurrentMapCacheManager();
    return new SpringRetentionPolicyCache(
        cacheManager.getCache(RetentionPolicyCache.CACHE_NAME), ttlSeconds, clock);
  }

  private static final class MutableClock extends Clock {
    private Instant instant;

    private MutableClock(Instant instant) {
      this.instant = instant;
    }

    void setInstant(Instant instant) {
      this.instant = instant;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
