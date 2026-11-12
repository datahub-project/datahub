package com.linkedin.gms.factory.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import org.testng.annotations.Test;

public class EntityGraphStatusMapConfigTest {

  @Test
  public void entityGraphStatusMapHasNoSizeEviction() {
    CacheConfig cacheConfig = new CacheConfig();
    EntityGraphCacheProperties properties =
        EntityGraphCacheProperties.builder()
            .eviction(
                EntityGraphCacheProperties.Eviction.builder()
                    .hazelcast(
                        EntityGraphCacheProperties.HazelcastEviction.builder()
                            .backupCount(1)
                            .build())
                    .build())
            .build();

    MapConfig mapConfig = cacheConfig.entityGraphStatusMapConfig(properties);

    assertEquals(mapConfig.getName(), EntityGraphCacheProperties.STATUS_MAP);
    assertEquals(mapConfig.getBackupCount(), 1);
    EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
    assertTrue(
        evictionConfig.getSize() > 10_000
            || evictionConfig.getEvictionPolicy() != EvictionPolicy.LFU,
        "entityGraphStatus must not use a small LFU cap");
  }
}
