package com.linkedin.metadata.config.entitygraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ConfigFile;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.Eviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphDefinition;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphPopulation;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.HazelcastEviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.LocalEviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.MemoryPressure;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.NearCache;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeNearCache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityGraphCacheConfigLoaderTest {

  private EntityGraphCacheConfigLoader loader;

  @BeforeMethod
  public void setUp() {
    loader = new EntityGraphCacheConfigLoader(new ObjectMapper(), new YAMLMapper());
  }

  @Test
  public void testJsonOverlayMergesDomainBounds() {
    EntityGraphCacheProperties base = new EntityGraphCacheProperties();
    base.setGraphs(new HashMap<>());
    base.getGraphs().put("domain", new GraphDefinition());
    loader.applyJsonOverlay(
        "{\"entityGraphCache\":{\"graphs\":{\"domain\":{\"bounds\":{\"maxVertices\":20000}}}}}",
        base,
        "test");

    assertEquals(
        base.getGraphs().get("domain").getBounds().getMaxVertices(), Integer.valueOf(20000));
  }

  @Test
  public void testConfigJsonPropertyMergesPopulationTiming() {
    EntityGraphCacheProperties base = springLikeBase();
    base.setConfigJson("{\"graphs\":{\"domain\":{\"population\":{\"intervalSeconds\":900}}}}");
    EntityGraphCacheProperties effective = loader.loadEffective(base);

    assertEquals(effective.getGraphs().get("domain").getPopulation().getIntervalSeconds(), 900);
  }

  @Test
  public void testDeprecatedMaxStaleSecondsAliasAcceptedInJsonOverlay() {
    EntityGraphCacheProperties base = springLikeBase();
    base.setConfigJson("{\"graphs\":{\"domain\":{\"population\":{\"maxStaleSeconds\":900}}}}");
    EntityGraphCacheProperties effective = loader.loadEffective(base);

    assertEquals(effective.getGraphs().get("domain").getPopulation().getIntervalSeconds(), 900);
  }

  @Test
  public void testJsonOverlayMergesPopulationTimingAfterBundledFile() {
    EntityGraphCacheProperties effective = loader.loadEffective(springLikeBase());
    assertEquals(effective.getGraphs().get("domain").getPopulation().getIntervalSeconds(), 600);

    loader.applyJsonOverlay(
        "{\"graphs\":{\"domain\":{\"population\":{\"intervalSeconds\":900}}}}",
        effective,
        EntityGraphCacheConfigLoader.ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV);

    assertEquals(effective.getGraphs().get("domain").getPopulation().getIntervalSeconds(), 900);
    loader.validate(effective);
  }

  @Test
  public void testJsonOnlyGraphsWhenConfigFileDisabled() {
    EntityGraphCacheProperties base = springLikeBase();
    base.getConfigFile().setEnabled(false);
    base.getEviction().setNearCache(minimalNearCache());
    loader.applyJsonOverlay(
        "{\"graphs\":{\"domain-primary\":"
            + "{"
            + "\"enabled\":true,"
            + "\"buildSource\":\"primary\","
            + "\"edges\":[{\"sourceEntityType\":\"domain\",\"destinationEntityType\":\"domain\",\"relationshipType\":\"IsPartOf\"}],"
            + "\"scope\":{\"mode\":\"PARTIAL\",\"maxDepth\":15},"
            + "\"population\":{\"strategy\":\"LAZY\",\"intervalSeconds\":300}"
            + "}}}",
        base,
        EntityGraphCacheConfigLoader.ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV);

    loader.validate(base);
    assertEquals(base.getGraphs().get("domain-primary").getPopulation().getIntervalSeconds(), 300);
  }

  @Test
  public void testInvalidJsonOverlayFails() {
    EntityGraphCacheProperties base = new EntityGraphCacheProperties();
    assertThrows(
        IllegalStateException.class, () -> loader.applyJsonOverlay("{not-json", base, "test"));
  }

  @Test
  public void testPerGraphNearCacheDroppedForFullScope() {
    EntityGraphCacheProperties effective = loader.loadEffective(springLikeBase());
    loader.applyJsonOverlay(
        "{\"graphs\":{\"domain\":{\"eviction\":{\"nearCache\":{\"enabled\":true,\"maxSize\":8}}}}}",
        effective,
        "test");

    loader.validate(effective);

    assertEquals(effective.getGraphs().get("domain").getEviction().getNearCache(), null);
  }

  @Test
  public void testReservedPartialGraphIdRejected() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph(
            EntityGraphCacheProperties.RESERVED_PARTIAL_GRAPH_ID, validPartialGraph());
    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testPerGraphNearCacheRetainedForPartialScope() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph("partialDomain", validPartialGraph());
    config
        .getGraphs()
        .get("partialDomain")
        .setEviction(
            EntityGraphCacheProperties.GraphEviction.builder()
                .nearCache(NearCache.builder().enabled(true).maxSize(8).build())
                .build());

    loader.validate(config);

    assertEquals(
        config.getGraphs().get("partialDomain").getEviction().getNearCache().getMaxSize(), 8);
  }

  @Test
  public void testLoadEffectiveFromBundledConfigFile() {
    EntityGraphCacheProperties effective = loader.loadEffective(springLikeBase());

    assertEquals(effective.getGraphs().get("domain").getBuildSource(), "search");
    assertEquals(effective.getGraphs().get("domain").getBounds().getMaxVertices(), 500);
    assertEquals(effective.getEviction().getNearCache().getFull().isEnabled(), true);
    assertEquals(effective.getEviction().getNearCache().getFull().getMaxSize(), 2);
    assertEquals(effective.getEviction().getNearCache().getPartial().isEnabled(), false);
    assertEquals(
        effective.getGraphs().get("domain").getPopulation().getStrategy(),
        PopulationStrategy.SCHEDULED);
    assertEquals(effective.getGraphs().get("domain").getPopulation().getIntervalSeconds(), 600);
    assertEquals(
        effective.getGraphs().get("domain").getBindings().getPolicyFieldTypes(), List.of("DOMAIN"));

    assertEquals(effective.getGraphs().get("glossary").getBuildSource(), "graph");
    assertEquals(effective.getGraphs().get("glossary").getScope().getMode(), ScopeMode.PARTIAL);
    assertEquals(effective.getGraphs().get("glossary").getScope().getMaxDepth(), 25);
    assertEquals(effective.getGraphs().get("glossary").getEdges().size(), 2);
    assertEquals(effective.getGraphs().get("glossary").getPopulation().getIntervalSeconds(), 1200);
    assertEquals(
        effective.getGraphs().get("glossary").getBounds().getMaxVertices(), Integer.valueOf(30000));
    assertEquals(
        effective.getGraphs().get("glossary").getBounds().getMaxEdges(), Integer.valueOf(45000));
    assertEquals(effective.getGraphs().get("glossary").getEviction(), null);

    assertEquals(effective.getGraphs().get("container").getBuildSource(), "graph");
    assertEquals(effective.getGraphs().get("container").getScope().getMode(), ScopeMode.PARTIAL);
    assertEquals(effective.getGraphs().get("container").getScope().getMaxDepth(), 12);
    assertEquals(effective.getGraphs().get("container").getEdges().size(), 1);
    assertEquals(effective.getGraphs().get("container").getPopulation().getIntervalSeconds(), 1200);
    assertEquals(
        effective.getGraphs().get("container").getBounds().getMaxVertices(), Integer.valueOf(5000));
    assertEquals(
        effective.getGraphs().get("container").getBounds().getMaxEdges(), Integer.valueOf(7500));

    assertEquals(effective.getGraphs().get("membership").getBuildSource(), "graph");
    assertEquals(effective.getGraphs().get("membership").getScope().getMode(), ScopeMode.FULL);
    assertEquals(effective.getGraphs().get("membership").getEdges().size(), 4);
    assertEquals(
        effective.getGraphs().get("membership").getPopulation().getStrategy(),
        PopulationStrategy.SCHEDULED);
    assertEquals(effective.getGraphs().get("membership").getPopulation().getIntervalSeconds(), 600);
    assertEquals(
        effective.getGraphs().get("membership").getBounds().getMaxVertices(),
        Integer.valueOf(21000));
    assertEquals(
        effective.getGraphs().get("membership").getBounds().getMaxEdges(), Integer.valueOf(60000));
  }

  @Test
  public void testValidationSkippedWhenCacheDisabled() {
    EntityGraphCacheProperties config = new EntityGraphCacheProperties();
    config.setEnabled(false);
    config.setGraphs(new HashMap<>());
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    config.getGraphs().put("invalid", graph);

    loader.validate(config);
  }

  @Test
  public void testScheduledPartialRejected() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph("partialDomain", validPartialGraph());
    config
        .getGraphs()
        .get("partialDomain")
        .getPopulation()
        .setStrategy(PopulationStrategy.SCHEDULED);

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testFullScopeMaxDepthRejected() {
    EntityGraphCacheProperties config = loader.loadEffective(springLikeBase());
    config.getGraphs().get("domain").getScope().setMaxDepth(15);

    IllegalStateException ex =
        org.testng.Assert.expectThrows(IllegalStateException.class, () -> loader.validate(config));
    assertTrue(ex.getMessage().contains("scope.maxDepth is not valid for FULL scope"));
  }

  @Test
  public void testPartialScopeAllowedWithLazy() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph("partialDomain", validPartialGraph());
    loader.validate(config);
  }

  @Test
  public void testMissingPopulationStrategyRejected() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph("domain-primary", validPartialGraph());
    config.getGraphs().get("domain-primary").setPopulation(new GraphPopulation());

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testMissingBuildSourceRejected() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph("domain-primary", validPartialGraph());
    config.getGraphs().get("domain-primary").setBuildSource(null);

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testPartialGraphBuildSourceAllowed() {
    EntityGraphCacheProperties config = enabledConfigWithGraph("partialGraph", validPartialGraph());
    config.getGraphs().get("partialGraph").setBuildSource("graph");
    loader.validate(config);
  }

  @Test
  public void testPartialSearchBuildSourceAllowed() {
    EntityGraphCacheProperties config =
        enabledConfigWithGraph("domain-search-partial", validPartialGraph());
    config.getGraphs().get("domain-search-partial").setBuildSource("search");

    loader.validate(config);
  }

  @Test
  public void testMissingEdgeConfigRejected() {
    EntityGraphCacheProperties config = enabledConfigWithGraph("empty", new GraphDefinition());
    config.getGraphs().get("empty").setEnabled(true);
    config.getGraphs().get("empty").setPopulation(lazyPopulation());

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testFilterFieldsOnPrimaryBuildSourceRejected() {
    EntityGraphCacheProperties config = enabledConfigWithGraph("bad", validPartialGraph());
    config.getGraphs().get("bad").getBindings().getFilterFields().add("domains.keyword");

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testInvalidMemoryPressureActionRejected() {
    EntityGraphCacheProperties config = springLikeBase();
    config.getEviction().setNearCache(minimalNearCache());
    config.getEviction().getMemoryPressure().setAction("INVALID_ACTION");

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testDuplicateFilterBindingsRejected() {
    EntityGraphCacheProperties config = springLikeBase();
    config.getEviction().setNearCache(minimalNearCache());
    config.getGraphs().put("a", graphWithFilterField("domains.keyword"));
    config.getGraphs().put("b", graphWithFilterField("domains.keyword"));

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  /** Mirrors {@code datahub.gms.entityGraphCache} defaults from application.yaml. */
  static EntityGraphCacheProperties springLikeBase() {
    EntityGraphCacheProperties base = new EntityGraphCacheProperties();
    base.setEnabled(true);
    ConfigFile configFile = new ConfigFile();
    configFile.setEnabled(true);
    configFile.setPath(EntityGraphCacheProperties.DEFAULT_CONFIG_FILE_PATH);
    base.setConfigFile(configFile);
    base.setEviction(springEvictionDefaults());
    base.setGraphs(new HashMap<>());
    return base;
  }

  static Eviction springEvictionDefaults() {
    return Eviction.builder()
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
        .build();
  }

  private static EntityGraphCacheProperties enabledConfigWithGraph(
      String graphId, GraphDefinition graph) {
    EntityGraphCacheProperties config = springLikeBase();
    config.getEviction().setNearCache(minimalNearCache());
    config.getGraphs().put(graphId, graph);
    return config;
  }

  private static ScopeNearCache minimalNearCache() {
    return ScopeNearCache.builder()
        .full(NearCache.builder().enabled(true).maxSize(1).build())
        .partial(NearCache.builder().enabled(false).maxSize(16).build())
        .build();
  }

  private static GraphDefinition validPartialGraph() {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource("primary");
    graph.setScope(
        EntityGraphCacheProperties.ScopeConfig.builder()
            .mode(ScopeMode.PARTIAL)
            .maxDepth(15)
            .build());
    graph.setPopulation(lazyPopulation());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("domain")
                .destinationEntityType("domain")
                .relationshipType("IsPartOf")
                .build()));
    EntityGraphCacheProperties.GraphBindings bindings =
        new EntityGraphCacheProperties.GraphBindings();
    bindings.setFilterFields(new ArrayList<>());
    graph.setBindings(bindings);
    return graph;
  }

  private static GraphPopulation lazyPopulation() {
    return EntityGraphCacheProperties.GraphPopulation.builder()
        .strategy(PopulationStrategy.LAZY)
        .intervalSeconds(300)
        .build();
  }

  @Test
  public void testDuplicatePolicyBindingsRejected() {
    EntityGraphCacheProperties config = springLikeBase();
    config.getEviction().setNearCache(minimalNearCache());
    config.getGraphs().put("a", graphWithPolicyField("DOMAIN"));
    config.getGraphs().put("b", graphWithPolicyField("DOMAIN"));

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testLineageAndTripletConfigRejectedTogether() {
    EntityGraphCacheProperties config = enabledConfigWithGraph("mixed", validPartialGraph());
    GraphDefinition graph = config.getGraphs().get("mixed");
    graph.setLineage(new EntityGraphCacheProperties.LineageEdgeConfig());

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testEntityTypesShorthandRequiresRelationshipType() {
    EntityGraphCacheProperties config = enabledConfigWithGraph("shorthand", new GraphDefinition());
    GraphDefinition graph = config.getGraphs().get("shorthand");
    graph.setEnabled(true);
    graph.setBuildSource("primary");
    graph.setScope(
        EntityGraphCacheProperties.ScopeConfig.builder()
            .mode(ScopeMode.PARTIAL)
            .maxDepth(15)
            .build());
    graph.setPopulation(lazyPopulation());
    graph.setEntityTypes(List.of("domain"));

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testBackgroundRebuildRequiresLazyFullScope() {
    EntityGraphCacheProperties config = enabledConfigWithGraph("domain-bg", validPartialGraph());
    GraphDefinition graph = config.getGraphs().get("domain-bg");
    graph.getScope().setMode(ScopeMode.FULL);
    graph.setBuildSource("search");
    graph.getPopulation().setRebuildExecution(RebuildExecution.BACKGROUND);

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  @Test
  public void testMissingTopLevelEvictionRejectedWhenEnabled() {
    EntityGraphCacheProperties config = new EntityGraphCacheProperties();
    config.setEnabled(true);
    config.setGraphs(new HashMap<>());
    config.getGraphs().put("domain", validPartialGraph());

    assertThrows(IllegalStateException.class, () -> loader.validate(config));
  }

  private static GraphDefinition graphWithPolicyField(String field) {
    GraphDefinition graph = validPartialGraph();
    graph.setBuildSource("search");
    graph.getScope().setMode(ScopeMode.FULL);
    if (graph.getBindings().getPolicyFieldTypes() == null) {
      graph.getBindings().setPolicyFieldTypes(new ArrayList<>());
    }
    graph.getBindings().getPolicyFieldTypes().add(field);
    return graph;
  }

  private static GraphDefinition graphWithFilterField(String field) {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource("search");
    graph.setScope(EntityGraphCacheProperties.ScopeConfig.builder().mode(ScopeMode.FULL).build());
    graph.setPopulation(lazyPopulation());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("domain")
                .destinationEntityType("domain")
                .relationshipType("IsPartOf")
                .build()));
    EntityGraphCacheProperties.GraphBindings bindings =
        new EntityGraphCacheProperties.GraphBindings();
    bindings.setFilterFields(new ArrayList<>());
    bindings.getFilterFields().add(field);
    graph.setBindings(bindings);
    return graph;
  }
}
