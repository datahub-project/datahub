package com.linkedin.gms.factory.common;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class CaffeineCacheConfig {

  @Value("${CACHE_TTL_SECONDS:600}")
  private int cacheTtlSeconds;

  @Value("${CACHE_MAX_SIZE:10000}")
  private int cacheMaxSize;

  @Bean
  public CacheManager cacheManager() {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setCaffeine(caffeineCacheBuilder());
    return cacheManager;
  }

  private Caffeine<Object, Object> caffeineCacheBuilder() {
    return Caffeine.newBuilder()
        .initialCapacity(100)
        .maximumSize(cacheMaxSize)
        .expireAfterAccess(cacheTtlSeconds, TimeUnit.SECONDS)
        .recordStats();
  }
}