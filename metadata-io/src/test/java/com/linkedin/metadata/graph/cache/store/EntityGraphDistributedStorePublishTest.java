package com.linkedin.metadata.graph.cache.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphDistributedStorePublishTest {

  private HazelcastInstance hazelcast;
  private EntityGraphDistributedStore store;
  private final AtomicLong listenerInvocations = new AtomicLong();

  @BeforeMethod
  public void setUp() {
    listenerInvocations.set(0);
    Config config = new Config();
    config.setInstanceName("entity-graph-store-test-" + java.util.UUID.randomUUID());
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
    when(definition.getGraphId()).thenReturn("domain");
    when(definition.getScope()).thenReturn(EntityGraphScope.builder().mode(ScopeMode.FULL).build());

    EntityGraphRegistry registry = mock(EntityGraphRegistry.class);
    when(registry.hasFullScopeGraphs()).thenReturn(true);
    when(registry.getGraphsById()).thenReturn(Map.of("domain", definition));
    when(registry.getDefinition("domain")).thenReturn(definition);

    store =
        new EntityGraphDistributedStore(
            hazelcast, registry, cacheKey -> listenerInvocations.incrementAndGet());
  }

  @AfterMethod
  public void tearDown() {
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
  }

  @Test
  public void publishStoresActiveOnlyOnSnapshotNotStatusMap() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot base = sampleSnapshot(cacheKey, 0);

    assertTrue(store.tryClaimRebuild(cacheKey, 60_000L));
    assertEquals(store.getStatus(cacheKey), CacheStatus.BUILDING);

    store.publish(base, CacheStatus.ACTIVE);
    assertEquals(store.getStatus(cacheKey), CacheStatus.ACTIVE);
    assertEquals(store.getSnapshot(cacheKey).getCacheStatus(), CacheStatus.ACTIVE.name());
    assertFalse(hazelcast.getMap(EntityGraphCacheProperties.STATUS_MAP).containsKey(cacheKey));
  }

  @Test
  public void freshBuildingLeaseBlocksImmediateReclaim() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);

    assertTrue(store.tryClaimRebuild(cacheKey, 60_000L));
    assertFalse(store.tryClaimRebuild(cacheKey, 60_000L));
  }

  @Test
  public void staleBuildingLeaseCanBeReclaimedExclusively() throws InterruptedException {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    long leaseBlockMillis = 60_000L;
    long staleAfterMillis = 50L;

    assertTrue(store.tryClaimRebuild(cacheKey, leaseBlockMillis));
    assertFalse(store.tryClaimRebuild(cacheKey, leaseBlockMillis));

    Thread.sleep(staleAfterMillis + 20);

    assertTrue(store.tryClaimRebuild(cacheKey, staleAfterMillis));
    assertFalse(store.tryClaimRebuild(cacheKey, staleAfterMillis));
  }

  @Test
  public void buildingWithoutRecordedAtIsReclaimable() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    hazelcast
        .getMap(EntityGraphCacheProperties.STATUS_MAP)
        .put(cacheKey, EntityGraphOperationalStatus.of(CacheStatus.BUILDING, null));

    assertTrue(store.tryClaimRebuild(cacheKey, 60_000L));
    assertFalse(store.tryClaimRebuild(cacheKey, 60_000L));
  }

  @Test
  public void markCooldownRecordsTimestampOnStatusEntry() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    store.markCooldown(cacheKey);

    EntityGraphOperationalStatus operational =
        (EntityGraphOperationalStatus)
            hazelcast.getMap(EntityGraphCacheProperties.STATUS_MAP).get(cacheKey);
    assertEquals(operational.cacheStatus(), CacheStatus.COOLDOWN);
    assertTrue(operational.getRecordedAtMillis() != null);
    assertTrue(store.getCooldownRecordedAt(cacheKey).isPresent());
  }

  @Test
  public void releaseRebuildClaimClearsBuildingLease() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    store.tryClaimRebuild(cacheKey, 60_000L);
    store.releaseRebuildClaim(cacheKey);
    assertEquals(store.getStatus(cacheKey), CacheStatus.ABSENT);
  }

  @Test
  public void markOverLimitWritesFailureTombstoneForSnapshotKey() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    store.publish(sampleSnapshot(cacheKey, 0), CacheStatus.ACTIVE);

    store.markOverLimit(cacheKey);

    assertEquals(store.getStatus(cacheKey), CacheStatus.OVER_LIMIT);
    EntityGraphSnapshot tombstone = store.getSnapshot(cacheKey);
    assertEquals(tombstone.getEdges().size(), 0);
    assertEquals(tombstone.getVertexCount(), 0);
    assertEquals(tombstone.getCacheStatus(), CacheStatus.OVER_LIMIT.name());
    assertEquals(
        tombstone.getTopologyFingerprint(), EntityGraphDistributedStore.TOMBSTONE_FINGERPRINT);
  }

  @Test
  public void markCooldownOnFailureMarkerDoesNotWriteTombstone() {
    String markerKey =
        EntityGraphCacheKeys.partialFailureMarkerKey(
            "domain", GraphSnapshotSource.PRIMARY, "urn:li:domain:root");
    store.markCooldown(markerKey);
    assertEquals(store.getStatus(markerKey), CacheStatus.COOLDOWN);
    assertEquals(
        hazelcast.getMap(EntityGraphCacheProperties.FULL_SNAPSHOTS_MAP).get(markerKey), null);
  }

  @Test
  public void shouldSkipPublishDoesNotSkipWhenCandidateCoverageNull() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot existing = sampleSnapshot(cacheKey, 1);
    store.publish(existing, CacheStatus.ACTIVE);

    EntityGraphSnapshot candidate =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("search")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(null)
            .edges(existing.getEdges())
            .build();

    assertFalse(store.shouldSkipPublish(cacheKey, candidate));
  }

  @Test
  public void publishEmbedsMonotonicGenerationInSnapshot() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot base = sampleSnapshot(cacheKey, 0);

    store.publish(base, CacheStatus.ACTIVE);
    assertEquals(store.getGeneration(cacheKey), 1L);
    assertEquals(store.getSnapshot(cacheKey).getGeneration(), 1L);
    assertEquals(store.getSnapshot(cacheKey).getCacheStatus(), CacheStatus.ACTIVE.name());
    assertEquals(store.getStatus(cacheKey), CacheStatus.ACTIVE);

    store.publish(base, CacheStatus.ACTIVE);
    assertEquals(store.getGeneration(cacheKey), 2L);
    assertEquals(store.getSnapshot(cacheKey).getGeneration(), 2L);
    assertEquals(listenerInvocations.get(), 2L);
  }

  @Test
  public void getGenerationReturnsZeroWhenSnapshotMissing() {
    assertEquals(
        store.getGeneration(
            EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH)),
        0L);
  }

  @Test
  public void tryClaimRebuildAllowsSingleWinner() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);

    assertTrue(store.tryClaimRebuild(cacheKey, 60_000L));
    assertEquals(store.getStatus(cacheKey), CacheStatus.BUILDING);
    assertFalse(store.tryClaimRebuild(cacheKey, 60_000L));
  }

  @Test
  public void dropFailureMarkerKeysForRootsClearsMarkerStatus() {
    String root = "urn:li:domain:root";
    String markerKey =
        EntityGraphCacheKeys.partialFailureMarkerKey("domain", GraphSnapshotSource.PRIMARY, root);
    store.markCooldown(markerKey);
    assertEquals(store.getStatus(markerKey), CacheStatus.COOLDOWN);

    store.dropFailureMarkerKeysForRoots("domain", GraphSnapshotSource.PRIMARY, List.of(root));
    assertEquals(store.getStatus(markerKey), CacheStatus.ABSENT);
  }

  @Test
  public void getStatusAbsentForUnknownKey() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    assertEquals(store.getStatus(cacheKey), CacheStatus.ABSENT);
  }

  @Test
  public void findCacheKeyForSeedsReusesLocalViewLookup() {
    String cacheKey =
        EntityGraphCacheKeys.componentCacheKey(
            "domain", GraphSnapshotSource.PRIMARY, "componentfp");
    EntityGraphSnapshot snapshot = sampleSnapshot(cacheKey, 1L);
    store.publish(snapshot, CacheStatus.ACTIVE);

    EntityGraphView cachedView = new EntityGraphView(snapshot.getEdges());
    assertEquals(
        store
            .findCacheKeyForSeeds(
                "domain",
                GraphSnapshotSource.PRIMARY,
                Set.of("urn:li:domain:root"),
                snap -> snap.getGeneration() == 1L ? Optional.of(cachedView) : Optional.empty())
            .orElse(null),
        cacheKey);
  }

  @Test
  public void publishNormalizesNullEdgesToEmptyList() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("search")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(0)
            .edgeCount(0)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .edges(null)
            .build();

    store.publish(snapshot, CacheStatus.ACTIVE);

    EntityGraphSnapshot published = store.getSnapshot(cacheKey);
    assertEquals(published.getEdges().size(), 0);
  }

  @Test
  public void invalidationGenerationIncrementsOnDropGraph() {
    assertEquals(store.getInvalidationGeneration("domain"), 0L);
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    store.publish(sampleSnapshot(cacheKey, 0), CacheStatus.ACTIVE);
    store.dropGraph("domain");
    assertEquals(store.getInvalidationGeneration("domain"), 1L);
  }

  @Test
  public void removeVertexFromSnapshotRemovesTwoVerticesSequentially() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("search")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:other")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .build();
    store.publish(snapshot, CacheStatus.ACTIVE);

    assertTrue(store.removeVertexFromSnapshot("domain", "urn:li:domain:child", 100));
    EntityGraphSnapshot afterFirst = store.getSnapshot(cacheKey);
    assertTrue(afterFirst != null);
    assertEquals(afterFirst.getVertexCount(), 2);

    assertTrue(store.removeVertexFromSnapshot("domain", "urn:li:domain:other", 100));
    assertTrue(store.getSnapshot(cacheKey) == null);
  }

  @Test
  public void removeVertexFromSnapshotConcurrentRemovesBothVertices() throws Exception {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("search")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:other")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .build();
    store.publish(snapshot, CacheStatus.ACTIVE);

    CyclicBarrier startBarrier = new CyclicBarrier(2);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicBoolean failed = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      executor.submit(
          () -> {
            try {
              startBarrier.await();
              store.removeVertexFromSnapshot("domain", "urn:li:domain:child", 100);
            } catch (Exception e) {
              failed.set(true);
            } finally {
              doneLatch.countDown();
            }
          });
      executor.submit(
          () -> {
            try {
              startBarrier.await();
              store.removeVertexFromSnapshot("domain", "urn:li:domain:other", 100);
            } catch (Exception e) {
              failed.set(true);
            } finally {
              doneLatch.countDown();
            }
          });
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
      assertFalse(failed.get());

      EntityGraphSnapshot after = store.getSnapshot(cacheKey);
      assertTrue(after == null || after.getVertexCount() == 1);
      if (after != null) {
        assertEquals(after.getVertexCount(), 1);
        assertEquals(after.getEdgeCount(), 0);
        assertTrue(after.getGeneration() >= 2L);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void removeVertexFromSnapshotClearsOverLimitWhenBelowMaxVertices() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey(cacheKey)
            .generation(0L)
            .buildSource("search")
            .builtAtMillis(System.currentTimeMillis())
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:other")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .build();
    store.publish(snapshot, CacheStatus.ACTIVE);
    hazelcast
        .getMap(EntityGraphCacheProperties.STATUS_MAP)
        .put(cacheKey, EntityGraphOperationalStatus.of(CacheStatus.OVER_LIMIT));
    assertEquals(store.getStatus(cacheKey), CacheStatus.OVER_LIMIT);

    assertTrue(store.removeVertexFromSnapshot("domain", "urn:li:domain:child", 3));

    assertEquals(store.getStatus(cacheKey), CacheStatus.ACTIVE);
    assertEquals(store.getSnapshot(cacheKey).getVertexCount(), 2);
  }

  @Test
  public void isRebuildLeaseHeldReflectsStatusMapOnly() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    assertFalse(store.isRebuildLeaseHeld(cacheKey));
    store.tryClaimRebuild(cacheKey, 60_000L);
    assertTrue(store.isRebuildLeaseHeld(cacheKey));
  }

  private static EntityGraphSnapshot sampleSnapshot(String cacheKey, long generation) {
    return EntityGraphSnapshot.builder()
        .graphId("domain")
        .cacheKey(cacheKey)
        .generation(generation)
        .buildSource("search")
        .builtAtMillis(System.currentTimeMillis())
        .vertexCount(2)
        .edgeCount(1)
        .topologyFingerprint("fp")
        .traversalCoverage(TraversalCoverage.fullComplete())
        .edges(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn("urn:li:domain:child")
                    .destinationUrn("urn:li:domain:root")
                    .relationshipType("IsPartOf")
                    .build()))
        .build();
  }
}
