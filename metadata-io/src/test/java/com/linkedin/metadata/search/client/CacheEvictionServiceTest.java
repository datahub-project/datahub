package com.linkedin.metadata.search.client;

import static com.linkedin.metadata.search.client.CachingEntitySearchService.*;
import static org.testng.Assert.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
}
