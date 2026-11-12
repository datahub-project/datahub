package com.linkedin.metadata.graph.cache.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotSerializer;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphDistributedStorePartialTest {

  private HazelcastInstance hazelcast;
  private EntityGraphDistributedStore store;

  @BeforeMethod
  public void setUp() {
    Config config = new Config();
    config.setInstanceName("entity-graph-partial-store-" + java.util.UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config
        .getSerializationConfig()
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphSnapshot.class)
                .setImplementation(new EntityGraphSnapshotSerializer()))
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphOperationalStatus.class)
                .setImplementation(new EntityGraphOperationalStatusSerializer()));
    hazelcast = Hazelcast.newHazelcastInstance(config);

    EntityGraphDefinition definition = mock(EntityGraphDefinition.class);
    when(definition.getGraphId()).thenReturn("glossary");
    when(definition.getScope())
        .thenReturn(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).build());

    EntityGraphRegistry registry = mock(EntityGraphRegistry.class);
    when(registry.hasFullScopeGraphs()).thenReturn(false);
    when(registry.getGraphsById()).thenReturn(Map.of("glossary", definition));
    when(registry.getDefinition("glossary")).thenReturn(definition);

    store = new EntityGraphDistributedStore(hazelcast, registry, key -> {});
  }

  @AfterMethod
  public void tearDown() {
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
  }

  @Test
  public void findCacheKeyForSeedsUsesSeedIndexAfterPublish() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp1");
    store.publish(sampleSnapshot(cacheKey), CacheStatus.ACTIVE);

    Optional<String> resolved =
        store.findCacheKeyForSeeds(
            "glossary", GraphSnapshotSource.GRAPH, Set.of("urn:li:glossaryNode:child"));

    assertTrue(resolved.isPresent());
    assertEquals(resolved.get(), cacheKey);
  }

  @Test
  public void dropPartialGraphClearsPartialMapAndStatus() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp1");
    store.publish(sampleSnapshot(cacheKey), CacheStatus.ACTIVE);
    store.markCooldown(cacheKey);

    assertEquals(store.getStatus(cacheKey), CacheStatus.COOLDOWN);
    assertTrue(store.getSnapshot(cacheKey) != null);

    store.dropPartialGraph("glossary");

    assertEquals(store.getStatus(cacheKey), CacheStatus.ABSENT);
    assertEquals(store.getSnapshot(cacheKey), null);
    assertEquals(store.getInvalidationGeneration("glossary"), 1L);
  }

  @Test
  public void anySnapshotStatusForGraphReturnsFirstNonAbsent() {
    String activeKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp-active");
    store.publish(sampleSnapshot(activeKey), CacheStatus.ACTIVE);

    assertEquals(store.anySnapshotStatusForGraph("glossary"), CacheStatus.ACTIVE);
  }

  @Test
  public void publishRejectsNonActiveStatus() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp");
    expectThrows(
        IllegalArgumentException.class,
        () -> store.publish(sampleSnapshot(cacheKey), CacheStatus.COOLDOWN));
  }

  @Test
  public void markInvalidWritesFailureTombstone() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp");
    store.publish(sampleSnapshot(cacheKey), CacheStatus.ACTIVE);

    store.markInvalid(cacheKey);

    assertEquals(store.getStatus(cacheKey), CacheStatus.INVALID);
    EntityGraphSnapshot tombstone = store.getSnapshot(cacheKey);
    assertEquals(tombstone.getCacheStatus(), CacheStatus.INVALID.name());
    assertEquals(
        tombstone.getTopologyFingerprint(), EntityGraphDistributedStore.TOMBSTONE_FINGERPRINT);
  }

  @Test
  public void shouldSkipPublishWhenTopologyUnchangedAndNoCoverageImprovement() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp");
    EntityGraphSnapshot existing = sampleSnapshot(cacheKey);
    store.publish(existing, CacheStatus.ACTIVE);

    EntityGraphSnapshot candidate =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("graph")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .edges(existing.getEdges())
            .build();

    assertTrue(store.shouldSkipPublish(cacheKey, candidate));
  }

  @Test
  public void shouldNotSkipPublishWhenCoverageStrictlyImproves() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp");
    TraversalCoverage shallow =
        TraversalCoverage.builder()
            .direction(
                TraversalCoverage.DirectionCoverage.builder()
                    .direction(com.linkedin.metadata.graph.cache.TraversalDirection.FORWARD)
                    .explored(true)
                    .complete(false)
                    .exploredDepth(1)
                    .configuredMaxDepth(15)
                    .build())
            .build();
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("graph")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(shallow)
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:glossaryNode:child")
                        .destinationUrn("urn:li:glossaryNode:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .build();
    store.publish(existing, CacheStatus.ACTIVE);

    EntityGraphSnapshot candidate =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("graph")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .edges(existing.getEdges())
            .build();

    assertFalse(store.shouldSkipPublish(cacheKey, candidate));
  }

  @Test
  public void getStatusFallsBackToAbsentForUnknownOperationalStatus() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey("glossary", GraphSnapshotSource.GRAPH, "fp");
    hazelcast
        .getMap(EntityGraphCacheProperties.STATUS_MAP)
        .put(
            cacheKey,
            new EntityGraphOperationalStatus("NOT_A_REAL_STATUS", System.currentTimeMillis()));

    assertEquals(store.getStatus(cacheKey), CacheStatus.ABSENT);
  }

  private static EntityGraphSnapshot sampleSnapshot(String cacheKey) {
    return EntityGraphSnapshot.builder()
        .graphId("glossary")
        .cacheKey(cacheKey)
        .generation(0L)
        .buildSource("graph")
        .builtAtMillis(System.currentTimeMillis())
        .vertexCount(2)
        .edgeCount(1)
        .topologyFingerprint("fp")
        .traversalCoverage(TraversalCoverage.fullComplete())
        .edges(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn("urn:li:glossaryNode:child")
                    .destinationUrn("urn:li:glossaryNode:root")
                    .relationshipType("IsPartOf")
                    .build()))
        .build();
  }
}
