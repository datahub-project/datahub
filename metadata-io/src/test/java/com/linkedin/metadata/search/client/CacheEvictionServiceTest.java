package com.linkedin.metadata.search.client;

import static com.linkedin.metadata.search.client.CachingEntitySearchService.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.javatuples.Octet;
import org.javatuples.Septet;
import org.jetbrains.annotations.NotNull;
import org.springframework.cache.CacheManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CacheEvictionServiceTest {

  CacheEvictionService evictionService;
  CacheManager cacheManager;

  CacheManager cacheManagerWithCaffeine;
  CacheManager cacheManagerWithHazelCast;

  // HazelcastInstance hazelcastInstance;

  int cacheKeyCount;
  // We cant use the spring Caffeine cache Manager in metadata-io due to a java 11 dependency --
  // this is not a problem  with the gms, but an issue with just the metadata-io jar and the
  // associated unit tests.
  final Map<String, Cache> nativeCacheMapForCaffeine = new HashMap<>();

  final String UNSUPPORTED_CACHE_NAME = "SampleUnsupportedCacheName";

  @BeforeClass
  void setupCacheManagers() {
    // hazelcastInstance = Hazelcast.newHazelcastInstance();
    // this.cacheManagerWithHazelCast = new HazelcastCacheManager(hazelcastInstance);

    // Not using the remaining cache methods in the unit tests.
    this.cacheManagerWithCaffeine =
        new CacheManager() {
          {
            Caffeine<Object, Object> caffeine =
                Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).maximumSize(2000);
            nativeCacheMapForCaffeine.put(
                ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME, caffeine.build());
            nativeCacheMapForCaffeine.put(UNSUPPORTED_CACHE_NAME, caffeine.build());
            nativeCacheMapForCaffeine.put(
                ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME, caffeine.build());
          }

          @Override
          public org.springframework.cache.Cache getCache(String name) {
            if (name.equals("missingcache")) {
              return null;
            } else {
              return new org.springframework.cache.Cache() {
                @Override
                public String getName() {
                  return name;
                }

                @Override
                public Object getNativeCache() {
                  return nativeCacheMapForCaffeine.get(name);
                }

                // Not using the remaining cache methods in the unit tests.
                @Override
                public ValueWrapper get(Object key) {
                  return null;
                }

                @Override
                public <T> T get(Object key, Class<T> type) {
                  return null;
                }

                @Override
                public <T> T get(Object key, Callable<T> valueLoader) {
                  return null;
                }

                @Override
                public void put(Object key, Object value) {}

                @Override
                public void evict(Object key) {
                  nativeCacheMapForCaffeine.get(name).invalidate(key);
                }

                @Override
                public void clear() {
                  nativeCacheMapForCaffeine.get(name).invalidateAll();
                }
              };
            }
          }

          @Override
          public Collection<String> getCacheNames() {
            return nativeCacheMapForCaffeine.keySet();
          }
        };
  }

  @BeforeMethod
  void setupCacheManager() {

    this.cacheManager = cacheManagerWithCaffeine;
    // prepare some cached results
    // For all tuple fields that we dont care about in this test, are initialised with null.
    Map<
            @NotNull Septet<Object, List<String>, String, String, Object, Object, Object>,
            @NotNull String>
        searchCacheData =
            Map.of(
                Septet.with(
                    null, // opContext
                    Arrays.asList("container", "dataset"), // entity matches but no urn in filter.
                    "*", // query
                    "{\"or\":[{\"and\":[{\"condition\":\"EQUAL\",\"negated\":false,\"field\":\"_entityType\",\"value\":\"\",\"values\":[\"CONTAINER\"]}]}]}",
                    // filters
                    null, // sort criteria
                    Arrays.asList("some facet json"),
                    null /* querySize*/),
                "allcontainers",
                Septet.with(
                    null,
                    Arrays.asList("dashboard", "container"),
                    "*",
                    "some json that contains container urn:li:container:foo",
                    null,
                    Arrays.asList("some facet json"),
                    null),
                "container.foo",
                Septet.with(
                    null,
                    Arrays.asList("dashboard", "container"),
                    "*",
                    "some json that contains unknown urn:li:container:bar",
                    null,
                    Arrays.asList("some facet json"),
                    null),
                "container.bar",
                Septet.with(
                    null,
                    Arrays.asList(
                        "dashboard", "container"), // entity match, but URN not a match in filter
                    "*",
                    "some json that contains unknown urn:li:dashboard:foobar",
                    null,
                    Arrays.asList("some facet json"),
                    null),
                "dashboard.foobar",
                Septet.with(
                    null,
                    Arrays.asList("structuredproperty"), // entity not matching
                    "*",
                    "{\"or\":[{\"and\":[{\"condition\":\"EQUAL\",\"negated\":false,\"field\":\"_entityType\",\"value\":\"\",\"values\":[\"CONTAINER\"]}]}]}",
                    null,
                    Arrays.asList("some facet json"),
                    null),
                "structuredproperty",
                Septet.with(
                    null,
                    Arrays.asList("container"), // entity match, but URN not a match in filter
                    "*",
                    null, // filter
                    null,
                    Arrays.asList("some facet json"),
                    null),
                "containeronly");

    Cache cache =
        (Cache) cacheManager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME).getNativeCache();
    cache.invalidateAll();
    for (Map.Entry entry : searchCacheData.entrySet()) {
      cache.put(entry.getKey(), entry.getValue());
    }

    Map<
            @NotNull Octet<
                Object, List<String>, String, String, Object, String, List<String>, Integer>,
            @NotNull String>
        scrollCacheData =
            Map.of(
                Octet.with(
                    null, // opContext
                    Arrays.asList("container", "dataset"), // entity matches but no urn in filter.
                    "*", // query
                    "{\"or\":[{\"and\":[{\"condition\":\"EQUAL\",\"negated\":false,\"field\":\"_entityType\",\"value\":\"\",\"values\":[\"CONTAINER\"]}]}]}",
                    // filters
                    null, // sort criteria
                    "scrollid",
                    Arrays.asList("some facet json"),
                    1 /* querySize*/),
                "allcontainers",
                Octet.with(
                    null, // opContext
                    Arrays.asList("container", "dataset"), // entity matches but no urn in filter.
                    "*", // query
                    null, // filters
                    null, // sort criteria
                    "scrollid",
                    Arrays.asList("some facet json"),
                    1 /* querySize*/),
                "allcontainers-null-filter");

    cacheKeyCount = searchCacheData.size();

    cache = (Cache) cacheManager.getCache(ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME).getNativeCache();
    cache.invalidateAll();
    for (Map.Entry entry : scrollCacheData.entrySet()) {
      cache.put(entry.getKey(), entry.getValue());
    }

    cache = (Cache) cacheManager.getCache(UNSUPPORTED_CACHE_NAME).getNativeCache();
    cache.invalidateAll();
    for (Map.Entry entry : searchCacheData.entrySet()) {
      cache.put(
          entry.getKey(),
          entry.getValue()); // oK to have the same values, this shouldn't even be looked up.
    }

    evictionService = new CacheEvictionService(cacheManager, true, true);
    this.cacheManager = cacheManager;
  }

  Map getAsMap(String cacheName) {
    // to inspect the cache for the test assertions
    com.github.benmanes.caffeine.cache.Cache<Object, Object> cache =
        (com.github.benmanes.caffeine.cache.Cache<Object, Object>)
            cacheManager.getCache(cacheName).getNativeCache();
    return cache.asMap();
  }

  @Test
  void testEntityTypeNotInCache() throws URISyntaxException {
    evictionService.evict(List.of(Urn.createFromString("urn:li:platform:foo")));

    Map cacheAsMap = getAsMap(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    assertEquals(cacheAsMap.size(), cacheKeyCount); // no evictions
    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), cacheKeyCount); // no evictions
  }

  @Test
  void testEntityTypeMatched() throws URISyntaxException {
    // Type in cache, but not urn
    evictionService.evict(List.of(Urn.createFromString("urn:li:container:dontmatch")));

    Map cacheAsMap = getAsMap(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    assertEquals(cacheAsMap.size(), cacheKeyCount - 2); // evictions
    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), cacheKeyCount);
    assertFalse(cacheAsMap.values().contains("allcontainers")); // show be evicted
    assertFalse(cacheAsMap.values().contains("containeronly")); // show be evicted

    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), cacheKeyCount); // no evictions
  }

  @Test
  void testEntityTypeAndUrnMatched() throws URISyntaxException {
    // type and urn in cache
    evictionService.evict(List.of(Urn.createFromString("urn:li:container:bar")));

    Map cacheAsMap = getAsMap(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    assertEquals(cacheAsMap.size(), cacheKeyCount - 3); //  evictions
    assertFalse(cacheAsMap.values().contains("allcontainers")); // evicted
    assertFalse(cacheAsMap.values().contains("container.bar")); // evicted
    assertFalse(cacheAsMap.values().contains("containeronly")); // evicted

    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), cacheKeyCount); // no evictions
  }

  @Test
  void testPerfWithLargeCache() throws URISyntaxException {
    Cache cache =
        (Cache) cacheManager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME).getNativeCache();
    for (int i = 0; i < 1000; i++) {
      Septet key =
          Septet.with(
              null,
              Arrays.asList("Non-matching-entity" + i),
              "*",
              "{\"or\":[{\"and\":[{\"condition\":\"EQUAL\",\"negated\":false,\"field\":\"_entityType\",\"value\":\"\",\"values\":[\"CONTAINER\"]}]}]}",
              null,
              Arrays.asList("some facet json"),
              null);
      String value = "structuredproperty" + i;

      cache.put(key, value);
    }
    evictionService.evict(List.of(Urn.createFromString("urn:li:container:bar")));

    Map cacheAsMap = getAsMap(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    assertEquals(cacheAsMap.size(), cacheKeyCount + 1000 - 3);
    assertFalse(cacheAsMap.values().contains("allcontainers")); // evicted
    assertFalse(cacheAsMap.values().contains("container.bar")); // evicted
    assertFalse(cacheAsMap.values().contains("containeronly")); // evicted
    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), cacheKeyCount);

    // Note, this was just to check timing
  }

  @Test
  void testInvalidateCache() {
    evictionService.invalidate(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    ;

    Map cacheAsMap = getAsMap(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
    assertEquals(cacheAsMap.size(), 0);

    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), cacheKeyCount); // no evictions

    evictionService.invalidateAll();
    ;
    assertEquals(getAsMap(UNSUPPORTED_CACHE_NAME).size(), 0);
  }

  @Test
  void testDumpCache() {
    evictionService.dumpCache("test");
  }

  @Test(expectedExceptions = AssertionError.class)
  void testInvalidCache() {
    evictionService.invalidate("missingcache");
  }

  @Test
  void testDisabledFlags() throws URISyntaxException {
    CacheEvictionService service = new CacheEvictionService(null, true, false);
    service.invalidateAll(); // should be no op though
    service.invalidate("anycache"); // should be no op
    service.evict(List.of(Urn.createFromString("urn:li:container:bar")));

    service = new CacheEvictionService(null, false, true);
    service.invalidateAll(); // should be no op though
    service.invalidate("anycache"); // should be no op
    service.evict(List.of(Urn.createFromString("urn:li:container:bar")));
  }

  @Test
  void testUnreadableKeysClearThatCacheAndEvictionContinues() throws URISyntaxException {
    IMap<Object, Object> unreadable = mock(IMap.class);
    when(unreadable.keySet())
        .thenThrow(new HazelcastSerializationException("Resolving class is not allowed"));
    org.springframework.cache.Cache searchCache = mock(org.springframework.cache.Cache.class);
    when(searchCache.getNativeCache()).thenReturn(unreadable);

    Octet<Object, List<String>, String, String, Object, String, List<String>, Integer> scrollKey =
        Octet.with(
            null, List.of("container"), "*", "urn:li:container:foo", null, null, List.of(), 10);
    Cache<Object, Object> scrollNative = Caffeine.newBuilder().build();
    scrollNative.put(scrollKey, "result");
    org.springframework.cache.Cache scrollCache = mock(org.springframework.cache.Cache.class);
    when(scrollCache.getNativeCache()).thenReturn(scrollNative);

    CacheManager manager = mock(CacheManager.class);
    when(manager.getCacheNames())
        .thenReturn(
            List.of(
                ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME, ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME));
    when(manager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME)).thenReturn(searchCache);
    when(manager.getCache(ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME)).thenReturn(scrollCache);

    new CacheEvictionService(manager, true, true)
        .evict(List.of(Urn.createFromString("urn:li:container:foo")));

    verify(searchCache).invalidate();
    verify(scrollCache).evict(scrollKey);
    verify(scrollCache, never()).invalidate();
  }

  @Test
  void testUnreadableHazelcastKeyIsSkippedWithoutClearingTheCache() throws URISyntaxException {
    String clusterName = "cache-eviction-test-" + UUID.randomUUID();
    Config config = new Config().setClusterName(clusterName).setInstanceName(clusterName);
    JoinConfig join = config.getNetworkConfig().getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(false);
    join.getAutoDetectionConfig().setEnabled(false);
    // Same shape as GMS: Guava is blocked, so a key holding a Guava list cannot be read back.
    config
        .getSerializationConfig()
        .setJavaSerializationFilterConfig(
            new JavaSerializationFilterConfig()
                .setWhitelist(new ClassFilter().addPrefixes("org.javatuples."))
                .setBlacklist(new ClassFilter().addPrefixes("com.google.common.")));
    HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
    try {
      IMap<Object, Object> map = instance.getMap(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME);
      String filter = "urn:li:container:foo";
      // Written by an older GMS that passed the caller's Guava list straight into the key.
      Object unreadableKey =
          Septet.with(null, ImmutableList.of("container"), "*", filter, null, List.of(), null);
      Object readableKey =
          Septet.with(
              null,
              new java.util.ArrayList<>(List.of("container")),
              "*",
              filter,
              null,
              List.of(),
              1);
      map.put(unreadableKey, "old");
      map.put(readableKey, "new");

      org.springframework.cache.Cache cache = mock(org.springframework.cache.Cache.class);
      when(cache.getNativeCache()).thenReturn(map);
      doAnswer(invocation -> map.remove(invocation.getArgument(0))).when(cache).evict(any());
      CacheManager manager = mock(CacheManager.class);
      when(manager.getCacheNames()).thenReturn(List.of(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME));
      when(manager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME)).thenReturn(cache);

      new CacheEvictionService(manager, true, true)
          .evict(List.of(Urn.createFromString("urn:li:container:foo")));

      verify(cache).evict(readableKey);
      verify(cache, never()).invalidate();
      assertEquals(map.size(), 1, "only the unreadable key should remain");
    } finally {
      instance.shutdown();
    }
  }
}
