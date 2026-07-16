package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import java.util.List;
import java.util.UUID;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class EntityGraphSnapshotSerializerTest {

  private HazelcastInstance hazelcast;

  @AfterMethod
  public void tearDown() {
    if (hazelcast != null) {
      hazelcast.shutdown();
      hazelcast = null;
    }
  }

  @Test
  public void roundTripPreservesSnapshotFields() {
    Config config = new Config();
    config.setInstanceName("entity-graph-serializer-test-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config
        .getSerializationConfig()
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphSnapshot.class)
                .setImplementation(new EntityGraphSnapshotSerializer()));

    hazelcast = Hazelcast.newHazelcastInstance(config);

    TraversalCoverage coverage =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(2)
                    .configuredMaxDepth(15)
                    .complete(true)
                    .build())
            .build();

    EntityGraphSnapshot original =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey("domain@search")
            .generation(3L)
            .buildSource("search")
            .builtAtMillis(1_700_000_000_000L)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("abc123")
            .traversalCoverage(coverage)
            .cacheStatus(CacheStatus.ACTIVE.name())
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .build();

    IMap<String, EntityGraphSnapshot> map = hazelcast.getMap("entity-graph-serializer-test");
    map.put(original.getCacheKey(), original);
    EntityGraphSnapshot restored = map.get(original.getCacheKey());

    assertNotNull(restored);
    assertEquals(restored.getGraphId(), original.getGraphId());
    assertEquals(restored.getCacheKey(), original.getCacheKey());
    assertEquals(restored.getGeneration(), original.getGeneration());
    assertEquals(restored.getBuildSource(), original.getBuildSource());
    assertEquals(restored.getBuiltAtMillis(), original.getBuiltAtMillis());
    assertEquals(restored.getVertexCount(), original.getVertexCount());
    assertEquals(restored.getEdgeCount(), original.getEdgeCount());
    assertEquals(restored.getTopologyFingerprint(), original.getTopologyFingerprint());
    assertEquals(restored.getCacheStatus(), original.getCacheStatus());
    assertNotNull(restored.getTraversalCoverage());
    assertEquals(restored.getEdges().size(), 1);
    assertEquals(restored.getEdges().get(0).getSourceUrn(), "urn:li:domain:child");
    assertEquals(EntityGraphSnapshotSerializer.SERIALIZER_VERSION, 1);
  }

  @Test
  public void roundTripPreservesSnapshotWithNullEdges() {
    Config config = new Config();
    config.setInstanceName("entity-graph-serializer-null-edges-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config
        .getSerializationConfig()
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphSnapshot.class)
                .setImplementation(new EntityGraphSnapshotSerializer()));

    hazelcast = Hazelcast.newHazelcastInstance(config);

    EntityGraphSnapshot original =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey("domain@search")
            .generation(1L)
            .buildSource("search")
            .builtAtMillis(1_700_000_000_000L)
            .vertexCount(0)
            .edgeCount(0)
            .topologyFingerprint("tombstone")
            .cacheStatus(CacheStatus.COOLDOWN.name())
            .edges(null)
            .build();

    IMap<String, EntityGraphSnapshot> map = hazelcast.getMap("entity-graph-serializer-null-edges");
    map.put(original.getCacheKey(), original);
    EntityGraphSnapshot restored = map.get(original.getCacheKey());

    assertNotNull(restored);
    assertEquals(restored.getEdges().size(), 0);
  }
}
