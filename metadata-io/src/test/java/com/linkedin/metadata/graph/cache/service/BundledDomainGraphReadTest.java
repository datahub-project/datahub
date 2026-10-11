package com.linkedin.metadata.graph.cache.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheConfigLoader;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ConfigFile;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.Eviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.HazelcastEviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.LocalEviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.MemoryPressure;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * End-to-end read path for the production bundled {@code entity-graph-cache.yaml} domain graph:
 * config loader → registry → cache service expand on {@code domain@search}.
 */
public class BundledDomainGraphReadTest {

  private static final String GRAPH_ID = "domain";
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.SEARCH;
  private static final String CACHE_KEY = EntityGraphCacheKeys.fullCacheKey(GRAPH_ID, SOURCE);
  private static final String ROOT = "urn:li:domain:root";
  private static final String CHILD = "urn:li:domain:child";
  private static final String GRANDCHILD = "urn:li:domain:grandchild";

  private EntityGraphRegistry registry;
  private EntityGraphDefinition definition;
  private EntityGraphDistributedStore distributedStore;
  private EntityGraphLocalViewCache localViews;
  private EntityGraphCacheService service;

  @BeforeMethod
  public void setUp() {
    EntityGraphCacheConfigLoader loader =
        new EntityGraphCacheConfigLoader(new ObjectMapper(), new YAMLMapper());
    EntityGraphCacheProperties properties = loader.loadEffective(springLikeBaseForBundledFile());
    registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));
    definition = registry.getDefinition(GRAPH_ID);

    distributedStore = mock(EntityGraphDistributedStore.class);
    localViews = mock(EntityGraphLocalViewCache.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    OperationContext systemOperationContext =
        TestOperationContexts.Builder.builder().buildSystemContext();

    when(distributedStore.tryClaimRebuild(any(), anyLong())).thenReturn(true);
    when(distributedStore.getInvalidationGeneration(any())).thenReturn(0L);

    service =
        new EntityGraphCacheService(
            properties,
            registry,
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            Executors.newSingleThreadExecutor());

    seedActiveSnapshot();
  }

  @Test
  public void bundledDomainGraphResolvesKnownBinding() {
    assertEquals(registry.resolveKnownGraph(KnownEntityGraph.DOMAIN).getGraphId(), GRAPH_ID);
    assertEquals(registry.resolveKnownGraph(KnownEntityGraph.DOMAIN).getSource(), SOURCE);
    assertEquals(definition.getBuildSource(), SOURCE);
  }

  @Test
  public void bundledDomainGraphExpandReverseReturnsDescendants() {
    GraphReadResult result =
        service.expand(
            GRAPH_ID,
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of(ROOT),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED);

    assertTrue(result.isHit());
    Set<String> expanded = result.verticesOrEmpty();
    assertTrue(expanded.contains(ROOT));
    assertTrue(expanded.contains(CHILD));
    assertTrue(expanded.contains(GRANDCHILD));
  }

  @Test
  public void bundledDomainGraphExpandForwardReturnsAncestors() {
    GraphReadResult result =
        service.expand(
            GRAPH_ID,
            SOURCE,
            TraversalDirection.FORWARD,
            Set.of(GRANDCHILD),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED);

    assertTrue(result.isHit());
    Set<String> expanded = result.verticesOrEmpty();
    assertTrue(expanded.contains(GRANDCHILD));
    assertTrue(expanded.contains(CHILD));
    assertTrue(expanded.contains(ROOT));
  }

  private void seedActiveSnapshot() {
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(GRANDCHILD)
                .destinationUrn(CHILD)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey(CACHE_KEY)
            .generation(1L)
            .buildSource(SOURCE.name().toLowerCase())
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(3)
            .edgeCount(2)
            .topologyFingerprint("bundled-test")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);
    when(localViews.get(CACHE_KEY, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));
  }

  /** Mirrors {@code datahub.gms.entityGraphCache} defaults from application.yaml. */
  private static EntityGraphCacheProperties springLikeBaseForBundledFile() {
    EntityGraphCacheProperties base = new EntityGraphCacheProperties();
    base.setEnabled(true);
    ConfigFile configFile = new ConfigFile();
    configFile.setEnabled(true);
    configFile.setPath(EntityGraphCacheProperties.DEFAULT_CONFIG_FILE_PATH);
    base.setConfigFile(configFile);
    base.setEviction(
        Eviction.builder()
            .local(
                LocalEviction.builder()
                    .enabled(true)
                    .maxViews(16)
                    .maxEstimatedBytes(268435456L)
                    .build())
            .memoryPressure(
                MemoryPressure.builder()
                    .enabled(true)
                    .checkIntervalSeconds(30)
                    .heapUsageThresholdPercent(85)
                    .action("EVICT_LOCAL_LRU")
                    .cooldownSeconds(120)
                    .hysteresisPercent(5)
                    .build())
            .hazelcast(
                HazelcastEviction.builder()
                    .evictionPolicy("MAX_SIZE")
                    .maxSizePerNode(32)
                    .maxSizePolicy("PER_NODE")
                    .heapMaxSizePercent(0)
                    .ttlSeconds(0)
                    .backupCount(1)
                    .build())
            .build());
    base.setGraphs(new HashMap<>());
    return base;
  }
}
