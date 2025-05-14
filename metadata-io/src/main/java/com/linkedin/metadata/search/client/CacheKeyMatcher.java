package com.linkedin.metadata.search.client;

public interface CacheKeyMatcher {
  boolean supportsCache(String cacheName);

  // Called for each supported cache, with each key
  boolean match(String cacheName, Object key);
}
