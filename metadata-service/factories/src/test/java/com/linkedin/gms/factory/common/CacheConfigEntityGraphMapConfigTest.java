package com.linkedin.gms.factory.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphDefinition;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.NearCache;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import java.lang.reflect.Method;
import org.testng.annotations.Test;

public class CacheConfigEntityGraphMapConfigTest {

  @Test
  public void fullSnapshotMapConfigUsesGlobalNearCacheAndMaxSizeEviction() throws Exception {
    EntityGraphCacheProperties properties = propertiesWithNearCache(true, false);

    MapConfig mapConfig = invokeFullSnapshotMapConfig(properties);

    assertEquals(mapConfig.getName(), EntityGraphCacheProperties.FULL_SNAPSHOTS_MAP);
    assertEquals(mapConfig.getBackupCount(), 2);
    assertNotNull(mapConfig.getNearCacheConfig());
    assertEquals(mapConfig.getNearCacheConfig().getEvictionConfig().getSize(), 4);
    EvictionConfig eviction = mapConfig.getEvictionConfig();
    assertEquals(eviction.getEvictionPolicy(), EvictionPolicy.LFU);
    assertEquals(eviction.getMaxSizePolicy(), MaxSizePolicy.PER_NODE);
    assertEquals(eviction.getSize(), 64);
  }

  @Test
  public void partialSnapshotMapConfigUsesPerGraphNearCacheWhenConfigured() throws Exception {
    EntityGraphCacheProperties properties = propertiesWithNearCache(false, true);
    GraphDefinition graph = partialGraphWithNearCache(12);

    MapConfig mapConfig = invokePartialSnapshotMapConfig("glossary", graph, properties);

    assertEquals(
        mapConfig.getName(), EntityGraphCacheProperties.partialSnapshotsMapName("glossary"));
    NearCacheConfig nearCache = mapConfig.getNearCacheConfig();
    assertNotNull(nearCache);
    assertEquals(nearCache.getEvictionConfig().getSize(), 12);
  }

  @Test
  public void neverEvictionPolicyDisablesMapSizeEviction() throws Exception {
    EvictionConfig eviction =
        invokeBuildEntityGraphEvictionConfig(
            EntityGraphCacheProperties.HazelcastEviction.builder()
                .evictionPolicy("NEVER")
                .maxSizePerNode(32)
                .build());

    assertEquals(eviction.getEvictionPolicy(), EvictionPolicy.NONE);
    assertEquals(eviction.getSize(), 0);
  }

  @Test
  public void heapMaxSizePercentOverridesEntryCountPolicy() throws Exception {
    EvictionConfig eviction =
        invokeBuildEntityGraphEvictionConfig(
            EntityGraphCacheProperties.HazelcastEviction.builder()
                .evictionPolicy("MAX_SIZE")
                .maxSizePolicy("PER_NODE")
                .maxSizePerNode(32)
                .heapMaxSizePercent(75)
                .build());

    assertEquals(eviction.getMaxSizePolicy(), MaxSizePolicy.USED_HEAP_PERCENTAGE);
    assertEquals(eviction.getSize(), 75);
  }

  @Test
  public void ttlSecondsAppliedWhenConfigured() throws Exception {
    EntityGraphCacheProperties properties = propertiesWithNearCache(true, false);
    properties.getEviction().getHazelcast().setTtlSeconds(3600);

    MapConfig mapConfig = invokeFullSnapshotMapConfig(properties);

    assertEquals(mapConfig.getTimeToLiveSeconds(), 3600);
  }

  @Test
  public void entryCountMaxSizePolicyHonored() throws Exception {
    EvictionConfig eviction =
        invokeBuildEntityGraphEvictionConfig(
            EntityGraphCacheProperties.HazelcastEviction.builder()
                .evictionPolicy("SIZE_AND_TTL")
                .maxSizePolicy("ENTRY_COUNT")
                .maxSizePerNode(128)
                .build());

    assertEquals(eviction.getMaxSizePolicy(), MaxSizePolicy.ENTRY_COUNT);
    assertEquals(eviction.getSize(), 128);
  }

  private static EntityGraphCacheProperties propertiesWithNearCache(
      boolean fullNearCacheEnabled, boolean partialNearCacheEnabled) {
    return EntityGraphCacheProperties.builder()
        .eviction(
            EntityGraphCacheProperties.Eviction.builder()
                .nearCache(
                    EntityGraphCacheProperties.ScopeNearCache.builder()
                        .full(NearCache.builder().enabled(fullNearCacheEnabled).maxSize(4).build())
                        .partial(
                            NearCache.builder().enabled(partialNearCacheEnabled).maxSize(8).build())
                        .build())
                .hazelcast(
                    EntityGraphCacheProperties.HazelcastEviction.builder()
                        .evictionPolicy("MAX_SIZE")
                        .maxSizePerNode(64)
                        .backupCount(2)
                        .build())
                .build())
        .build();
  }

  private static GraphDefinition partialGraphWithNearCache(int maxSize) {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setScope(
        EntityGraphCacheProperties.ScopeConfig.builder()
            .mode(ScopeMode.PARTIAL)
            .maxDepth(15)
            .build());
    graph.setEviction(
        EntityGraphCacheProperties.GraphEviction.builder()
            .nearCache(NearCache.builder().enabled(true).maxSize(maxSize).build())
            .build());
    return graph;
  }

  private static MapConfig invokeFullSnapshotMapConfig(EntityGraphCacheProperties properties)
      throws Exception {
    Method method =
        CacheConfig.class.getDeclaredMethod(
            "buildEntityGraphFullSnapshotMapConfig", EntityGraphCacheProperties.class);
    method.setAccessible(true);
    return (MapConfig) method.invoke(null, properties);
  }

  private static MapConfig invokePartialSnapshotMapConfig(
      String graphId, GraphDefinition graph, EntityGraphCacheProperties properties)
      throws Exception {
    Method method =
        CacheConfig.class.getDeclaredMethod(
            "buildEntityGraphPartialSnapshotMapConfig",
            String.class,
            GraphDefinition.class,
            EntityGraphCacheProperties.class);
    method.setAccessible(true);
    return (MapConfig) method.invoke(null, graphId, graph, properties);
  }

  private static EvictionConfig invokeBuildEntityGraphEvictionConfig(
      EntityGraphCacheProperties.HazelcastEviction hazelcastEviction) throws Exception {
    Method method =
        CacheConfig.class.getDeclaredMethod(
            "buildEntityGraphEvictionConfig", EntityGraphCacheProperties.HazelcastEviction.class);
    method.setAccessible(true);
    return (EvictionConfig) method.invoke(null, hazelcastEviction);
  }
}
