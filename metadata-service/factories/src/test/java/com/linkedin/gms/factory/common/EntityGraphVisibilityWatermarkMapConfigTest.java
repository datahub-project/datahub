package com.linkedin.gms.factory.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import org.testng.annotations.Test;

public class EntityGraphVisibilityWatermarkMapConfigTest {

  @Test
  public void watermarkMapTakesTheSnapshotTtlAndIsNotSizeEvicted() {
    CacheConfig cacheConfig = new CacheConfig();
    EntityGraphCacheProperties properties =
        EntityGraphCacheProperties.builder()
            .eviction(
                EntityGraphCacheProperties.Eviction.builder()
                    .hazelcast(
                        EntityGraphCacheProperties.HazelcastEviction.builder()
                            .backupCount(1)
                            .ttlSeconds(1200)
                            .build())
                    .build())
            .build();

    MapConfig mapConfig = cacheConfig.entityGraphVisibilityWatermarkMapConfig(properties);

    assertEquals(mapConfig.getName(), EntityGraphCacheProperties.VISIBILITY_WATERMARK_MAP);
    assertEquals(mapConfig.getBackupCount(), 1);
    assertEquals(mapConfig.getTimeToLiveSeconds(), 1200);
    assertNotEquals(mapConfig.getEvictionConfig().getEvictionPolicy(), EvictionPolicy.LFU);
  }
}
