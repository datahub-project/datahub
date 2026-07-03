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
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.client.HierarchyReadSpecs;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
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

/** End-to-end read path for bundled {@code glossary@graph} PARTIAL hierarchy graph. */
public class BundledGlossaryGraphReadTest {

  private static final String GRAPH_ID = "glossary";
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.GRAPH;
  private static final String CACHE_KEY = "glossary@graph:component-test";
  private static final String ROOT_NODE = "urn:li:glossaryNode:root";
  private static final String CHILD_NODE = "urn:li:glossaryNode:child";
  private static final String TERM = "urn:li:glossaryTerm:term";

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
  public void bundledGlossaryGraphResolvesKnownBindingAndHierarchySpec() {
    EntityGraphBinding binding = registry.resolveKnownGraph(KnownEntityGraph.GLOSSARY);
    assertEquals(binding.getGraphId(), GRAPH_ID);
    assertEquals(binding.getSource(), SOURCE);
    assertTrue(
        HierarchyReadSpecs.forKnownGraph(KnownEntityGraph.GLOSSARY, binding, definition)
            .isPresent());
    assertEquals(
        HierarchyReadSpecs.forKnownGraph(KnownEntityGraph.GLOSSARY, binding, definition)
            .get()
            .getScrollSourceEntityTypes(),
        Set.of("glossaryNode", "glossaryTerm"));
  }

  @Test
  public void bundledGlossaryGraphExpandForwardFromTermIncludesNodeAncestors() {
    GraphReadResult result =
        service.expand(
            GRAPH_ID,
            SOURCE,
            TraversalDirection.FORWARD,
            Set.of(TERM),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED);

    assertTrue(result.isHit());
    Set<String> expanded = result.verticesOrEmpty();
    assertTrue(expanded.contains(TERM));
    assertTrue(expanded.contains(CHILD_NODE));
    assertTrue(expanded.contains(ROOT_NODE));
  }

  @Test
  public void bundledGlossaryGraphExpandReverseFromRootIncludesDescendants() {
    GraphReadResult result =
        service.expand(
            GRAPH_ID,
            SOURCE,
            TraversalDirection.REVERSE,
            Set.of(ROOT_NODE),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED);

    assertTrue(result.isHit());
    Set<String> expanded = result.verticesOrEmpty();
    assertTrue(expanded.contains(ROOT_NODE));
    assertTrue(expanded.contains(CHILD_NODE));
    assertTrue(expanded.contains(TERM));
  }

  @Test
  public void bundledGlossaryGraphWalkOrderedAncestorsFromTermUsesCache() {
    AncestorWalkResult result =
        service.walkOrderedForwardAncestors(GRAPH_ID, SOURCE, TERM, 10, ReadMode.CACHED);

    assertTrue(result.isHit());
    assertEquals(result.ancestorsOrEmpty(), List.of(CHILD_NODE, ROOT_NODE));
  }

  private void seedActiveSnapshot() {
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD_NODE)
                .destinationUrn(ROOT_NODE)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(TERM)
                .destinationUrn(CHILD_NODE)
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
            .topologyFingerprint("glossary-bundled-test")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .cacheStatus(CacheStatus.ACTIVE.name())
            .build();

    when(distributedStore.findCacheKeyForSeeds(any(), any(), any(), any()))
        .thenReturn(Optional.of(CACHE_KEY));
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(snapshot);
    when(distributedStore.getGeneration(CACHE_KEY)).thenReturn(1L);
    when(localViews.get(CACHE_KEY, 1L)).thenReturn(Optional.of(new EntityGraphView(edges)));
  }

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
