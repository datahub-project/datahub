package com.linkedin.gms.factory.common;

import static com.linkedin.metadata.search.client.CachingEntitySearchService.ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import com.linkedin.metadata.buffer.HazelcastCoalesceBuffer;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.search.EntityLineageResultCacheKey;
import com.linkedin.metadata.search.cache.CacheableSearcher;
import com.linkedin.metadata.search.cache.CachedEntityLineageResult;
import com.linkedin.metadata.search.client.CacheEvictionService;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.cache.KeyAspectEntityCountCacheKey;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.Refill;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.grid.hazelcast.Bucket4jHazelcast;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.javatuples.Septet;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Two filtered members exchanging the values DataHub stores in Hazelcast. Many keys are used so
 * that some partitions (and their backups) live on the other member and every value crosses the
 * wire.
 */
public class HazelcastSerializationFilterClusterTest {

  private static final int KEYS = 50;

  private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
  private HazelcastInstance member1;
  private HazelcastInstance member2;

  @BeforeClass
  public void startCluster() {
    member1 = factory.newHazelcastInstance(filteredConfig());
    member2 = factory.newHazelcastInstance(filteredConfig());
    assertEquals(member2.getCluster().getMembers().size(), 2);
  }

  @AfterClass
  public void stopCluster() {
    factory.terminateAll();
  }

  @Test
  public void searchCacheValuesAndKeyScansWork() {
    Cache cache1 =
        new HazelcastCacheManager(member1).getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    HazelcastCacheManager cacheManager2 = new HazelcastCacheManager(member2);
    Cache cache2 = cacheManager2.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);

    for (int i = 0; i < KEYS; i++) {
      cache1.put(searchKey(i), "{\"from\":" + i + "}");
    }
    for (int i = 0; i < KEYS; i++) {
      assertEquals(cache2.get(searchKey(i), String.class), "{\"from\":" + i + "}");
    }

    // Eviction scans and deserializes every key on the calling member.
    new CacheEvictionService(cacheManager2, true, true).evict(List.of(new CorpuserUrn("user0")));
    assertTrue(cache2.get(searchKey(0)) == null);
    assertNotNull(cache2.get(searchKey(1)));
  }

  @Test
  public void lineageCacheWorks() {
    Cache cache1 = new HazelcastCacheManager(member1).getCache("relationshipSearchService");
    Cache cache2 = new HazelcastCacheManager(member2).getCache("relationshipSearchService");

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    relationships.add(new LineageRelationship().setEntity(new CorpuserUrn("user")).setType("t"));
    EntityLineageResult lineage =
        new EntityLineageResult().setRelationships(relationships).setStart(0).setCount(1);

    for (int i = 0; i < KEYS; i++) {
      cache1.put(lineageKey(i), new CachedEntityLineageResult(lineage, i));
    }
    for (int i = 0; i < KEYS; i++) {
      assertEquals(
          cache2.get(lineageKey(i), CachedEntityLineageResult.class).getEntityLineageResult(),
          lineage);
    }
  }

  @Test
  public void retentionBufferMergesAcrossMembers() {
    CoalesceBuffer<RetentionKey, Long> buffer1 =
        new HazelcastCoalesceBuffer<>(member1, "retentionPending", "retentionLocks", null);
    CoalesceBuffer<RetentionKey, Long> buffer2 =
        new HazelcastCoalesceBuffer<>(member2, "retentionPending", "retentionLocks", null);

    for (int i = 0; i < KEYS; i++) {
      buffer1.merge(retentionKey(i), 1L, CoalesceBuffers.KEEP_MAX_LONG);
      buffer2.merge(retentionKey(i), 5L, CoalesceBuffers.KEEP_MAX_LONG);
    }

    List<Map.Entry<RetentionKey, Long>> drained = buffer2.drain(KEYS * 2);
    assertEquals(drained.size(), KEYS);
    drained.forEach(e -> assertEquals(e.getValue(), Long.valueOf(5L)));
  }

  @Test
  public void keyAspectCountCacheWorks() {
    IMap<KeyAspectEntityCountCacheKey, KeyAspectEntityCountResult> map1 =
        member1.getMap("keyAspectEntityCounts");
    IMap<KeyAspectEntityCountCacheKey, KeyAspectEntityCountResult> map2 =
        member2.getMap("keyAspectEntityCounts");
    KeyAspectEntityCountResult result =
        new KeyAspectEntityCountResult(
            List.of(new KeyAspectEntityCountEntry("dataset", "datasetKey", 3, 1)),
            List.of("dataset"),
            Instant.ofEpochMilli(1_000L),
            false);

    for (int i = 0; i < KEYS; i++) {
      map1.put(KeyAspectEntityCountCacheKey.of("ctx" + i, List.of("dataset")), result);
    }
    for (int i = 0; i < KEYS; i++) {
      assertEquals(
          map2.get(KeyAspectEntityCountCacheKey.of("ctx" + i, List.of("dataset"))), result);
    }
    assertEquals(map2.keySet().size(), KEYS);
  }

  @Test
  public void distributedRateLimitBucketsWork() {
    BucketConfiguration configuration =
        BucketConfiguration.builder()
            .addLimit(Bandwidth.classic(3, Refill.intervally(3, Duration.ofHours(1))))
            .buildConfiguration();
    ProxyManager<String> proxy1 =
        Bucket4jHazelcast.entryProcessorBasedBuilder(member1.<String, byte[]>getMap("rateLimit"))
            .build();
    ProxyManager<String> proxy2 =
        Bucket4jHazelcast.entryProcessorBasedBuilder(member2.<String, byte[]>getMap("rateLimit"))
            .build();

    for (int i = 0; i < KEYS; i++) {
      String key = "actor" + i;
      assertTrue(proxy1.builder().build(key, () -> configuration).tryConsume(2));
      assertTrue(proxy2.builder().build(key, () -> configuration).tryConsume(1));
      assertFalse(proxy2.builder().build(key, () -> configuration).tryConsume(1));
    }
  }

  private static Config filteredConfig() {
    Config config = new Config();
    HazelcastSerializationFilter.apply(config, List.of(), null, null);
    return config;
  }

  private static Septet<String, List<String>, String, String, String, List<String>, Object>
      searchKey(int i) {
    return Septet.with(
        "ctx",
        new ArrayList<>(List.of("corpuser")),
        "*",
        "{\"urn\":\"urn:li:corpuser:user" + i + "\"}",
        null,
        new ArrayList<>(),
        new CacheableSearcher.QueryPagination(i, 10));
  }

  private static EntityLineageResultCacheKey lineageKey(int i) {
    return new EntityLineageResultCacheKey(
        "ctx", new CorpuserUrn("user" + i), LineageDirection.DOWNSTREAM, 1, null);
  }

  private static RetentionKey retentionKey(int i) {
    return new SimpleRetentionKey("urn:li:corpuser:user" + i, "corpUserInfo");
  }
}
