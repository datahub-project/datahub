package com.linkedin.metadata.search.client;

import com.hazelcast.map.IMap;
import com.linkedin.common.urn.Urn;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

@RequiredArgsConstructor
@Slf4j
public class CacheEvictionService {
  private final CacheManager cacheManager;
  private final Boolean cachingEnabled;
  private final Boolean enableEviction;

  // invalidates all caches
  public void invalidateAll() {
    if (cachingEnabled && enableEviction) {
      cacheManager.getCacheNames().forEach(this::invalidate);
    }
  }

  // invalidates a specific cache
  public void invalidate(String cacheName) {
    if (cachingEnabled && enableEviction) {
      Cache cache = cacheManager.getCache(cacheName);
      if (cache != null) {
        cache.invalidate();
      } else {
        throw new AssertionError(String.format("Invalid cache name %s supplied", cacheName));
      }
    }
  }

  public void evict(CacheKeyMatcher matcher){

    if (cachingEnabled && enableEviction) {
      Collection<String> cacheNames = cacheManager.getCacheNames();
      for (String cacheName : cacheNames) {
        long evictCount = 0;
        if (matcher.supportsCache(cacheName)) {
          Cache cache = cacheManager.getCache(cacheName);
          assert (cache != null);
          Set<Object> keys = getKeys(cacheName);
          for (Object key : keys) {
            if (matcher.match(cacheName, key)) {
              cache.evict(key);
              evictCount ++;
              log.debug("From cache '{}' evicting key {}", cacheName, key);
            }
          }
          if (evictCount>0){
            log.info("Evicted {} keys from cache {}", evictCount, cacheName);
          }
        }
      }
    }
  }

  private Set<Object> getKeys(String cacheName) {
    // Enumerating cache keys is not part of the standard Cache interface, but needs is native cache
    // implementation
    // dependent and so must be implemented for all cache implementations we may use.

    Cache springCache = cacheManager.getCache(cacheName);
    if (springCache == null) {
      return Collections.emptySet();
    }

    Object nativeCache = springCache.getNativeCache();
    if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache) {
      com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineCache =
          (com.github.benmanes.caffeine.cache.Cache<Object, Object>) nativeCache;
      return caffeineCache.asMap().keySet();
    } else if (nativeCache instanceof IMap) {
      IMap<Object, Object> hazelCache = (IMap<Object, Object>) nativeCache;
      return hazelCache.keySet();
    }

    log.warn("Unhandled cache type {} of type {}", cacheName, nativeCache.getClass());
    return Collections.emptySet();
  }

  //Useful during matcher debugging, but voluminous
  private void dumpCache(String message){
    log.debug("Begin Dump {}", message);
    cacheManager.getCacheNames()
        .forEach(cacheName -> {
          log.debug("Dump cache:  {}", cacheName);
          Cache cache  = cacheManager.getCache(cacheName);
          getKeys(cacheName).forEach(key -> {
            log.debug("  key {} : {}", key, cache.get(key));
          });
        });
  }

  public void evict(List<Urn> urns) {
    log.info("Attempting eviction of search cache due to updates to {}", urns);
    UrnCacheKeyMatcher matcher = new UrnCacheKeyMatcher(urns);
    evict(matcher);
  }
}
