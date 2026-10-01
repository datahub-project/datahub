package com.linkedin.common.client;

import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.cache.client.ClientCacheConfig;
import java.time.Duration;
import java.util.Map;
import org.testng.annotations.Test;

public class ClientCacheExpiryTest {

  @Test
  public void ttlAdjustmentChangesCreateExpiry() {
    ClientCacheConfig config =
        new ClientCacheConfig() {
          @Override
          public String getName() {
            return "test";
          }

          @Override
          public boolean isEnabled() {
            return true;
          }

          @Override
          public boolean isStatsEnabled() {
            return false;
          }

          @Override
          public int getStatsIntervalSeconds() {
            return 0;
          }

          @Override
          public int getDefaultTTLSeconds() {
            return 100;
          }

          @Override
          public int getMaxBytes() {
            return 1_000_000;
          }
        };

    ClientCache<String, String, ClientCacheConfig> cache =
        ClientCache.<String, String, ClientCacheConfig>builder()
            .config(config)
            .weigher((key, value) -> value.length())
            .loadFunction(keys -> Map.of())
            .ttlSecondsFunction((ignored, key) -> 100)
            .ttlAdjustment((value, configured) -> "miss".equals(value) ? 5 : configured)
            .build(null, ClientCacheExpiryTest.class);

    cache.getCache().put("hit", "value");
    cache.getCache().put("miss", "miss");

    Duration hitTtl =
        cache
            .getCache()
            .policy()
            .expireVariably()
            .orElseThrow()
            .getExpiresAfter("hit")
            .orElseThrow();
    Duration missTtl =
        cache
            .getCache()
            .policy()
            .expireVariably()
            .orElseThrow()
            .getExpiresAfter("miss")
            .orElseThrow();
    assertTrue(hitTtl.getSeconds() >= 90);
    assertTrue(missTtl.compareTo(Duration.ofSeconds(3)) > 0);
    assertTrue(missTtl.compareTo(Duration.ofSeconds(6)) < 0);
  }
}
