package com.linkedin.metadata.graph.cache.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphDefinition;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphPopulation;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.client.HierarchyReadSpecs;
import com.linkedin.metadata.graph.cache.client.MembershipReadSpecs;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class EntityGraphRegistryTest {

  @Test
  public void testBuildResolvesKnownDomainGraph() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    EntityGraphBinding domain = registry.resolveKnownGraph(KnownEntityGraph.DOMAIN);
    assertNotNull(domain);
    assertEquals(domain.getGraphId(), "domain");
    assertEquals(domain.getSource(), GraphSnapshotSource.SEARCH);
  }

  @Test
  public void testBuildResolvesKnownGlossaryGraph() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    EntityGraphBinding glossary = registry.resolveKnownGraph(KnownEntityGraph.GLOSSARY);
    assertNotNull(glossary);
    assertEquals(glossary.getGraphId(), "glossary");
    assertEquals(glossary.getSource(), GraphSnapshotSource.GRAPH);
    assertTrue(
        HierarchyReadSpecs.forKnownGraph(
                KnownEntityGraph.GLOSSARY, glossary, registry.getDefinition("glossary"))
            .isPresent());
  }

  @Test
  public void testBuildResolvesKnownContainerGraph() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    EntityGraphBinding container = registry.resolveKnownGraph(KnownEntityGraph.CONTAINER);
    assertNotNull(container);
    assertEquals(container.getGraphId(), "container");
    assertEquals(container.getSource(), GraphSnapshotSource.GRAPH);
    assertTrue(
        HierarchyReadSpecs.forKnownGraph(
                KnownEntityGraph.CONTAINER, container, registry.getDefinition("container"))
            .isPresent());
  }

  @Test
  public void testBuildResolvesKnownMembershipGraph() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    EntityGraphBinding membership = registry.resolveKnownGraph(KnownEntityGraph.MEMBERSHIP);
    assertNotNull(membership);
    assertEquals(membership.getGraphId(), "membership");
    assertEquals(membership.getSource(), GraphSnapshotSource.GRAPH);
    assertTrue(
        MembershipReadSpecs.forKnownGraph(KnownEntityGraph.MEMBERSHIP, membership).isPresent());
  }

  @Test
  public void testMissingKnownGraphFailsWhenEnabled() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties.getGraphs().remove("domain");

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                EntityGraphRegistry.build(
                    properties,
                    new TestEntityRegistry(),
                    new LineageRegistry(new TestEntityRegistry())));
    assertTrue(ex.getMessage().contains("domain"));
    assertTrue(ex.getMessage().contains("DOMAIN"));
  }

  @Test
  public void testWrongBuildSourceForKnownGraphFailsWhenEnabled() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties.getGraphs().get("domain").setBuildSource("graph");

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                EntityGraphRegistry.build(
                    properties,
                    new TestEntityRegistry(),
                    new LineageRegistry(new TestEntityRegistry())));
    assertTrue(ex.getMessage().contains("DOMAIN"));
    assertTrue(ex.getMessage().contains("buildSource search"));
  }

  @Test
  public void testBuildResolvesSearchPartialDefinition() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    GraphDefinition graph = validGraphDefinition("search");
    graph.getScope().setMode(ScopeMode.PARTIAL);
    graph.getScope().setMaxDepth(15);
    properties.getGraphs().put("domain-search-partial", graph);

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    assertNotNull(registry.getDefinition("domain-search-partial"));
    assertEquals(
        registry.getDefinition("domain-search-partial").getBuildSource(),
        GraphSnapshotSource.SEARCH);
    assertEquals(
        registry.getDefinition("domain-search-partial").getScope().getMode(), ScopeMode.PARTIAL);
  }

  @Test
  public void testBuildResolvesGraphPartialDefinition() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    GraphDefinition graph = validGraphDefinition("graph");
    graph.getScope().setMode(ScopeMode.PARTIAL);
    properties.getGraphs().put("domain-graph", graph);

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    assertNotNull(registry.getDefinition("domain-graph"));
    assertEquals(
        registry.getDefinition("domain-graph").getBuildSource(), GraphSnapshotSource.GRAPH);
    assertEquals(registry.getDefinition("domain-graph").getScope().getMode(), ScopeMode.PARTIAL);
  }

  @Test
  public void testZeroResolvedEdgesFailsWhenEnabled() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties.getGraphs().put("bad", invalidEntityGraph());

    assertThrows(
        IllegalStateException.class,
        () ->
            EntityGraphRegistry.build(
                properties,
                new TestEntityRegistry(),
                new LineageRegistry(new TestEntityRegistry())));
  }

  @Test
  public void testCandidateGraphIdsForSourceAndDestinationEntityTypes() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    Set<String> candidates = registry.getCandidateGraphIds("domain", "domainProperties");
    assertTrue(candidates.contains("domain"));
    assertEquals(
        registry.getRelationshipTypesForAspect("domain", "domainProperties"), Set.of("IsPartOf"));
    assertFalse(registry.getCandidateGraphIds("domain", "institutionalMemory").contains("domain"));
    assertEquals(registry.getGraphIdsForEntityType("domain"), Set.of("domain"));
    assertTrue(registry.getGraphIdsForEntityType("dataset").isEmpty());
  }

  @Test
  public void testCandidateGraphIdsForNativeGroupMembership() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    assertTrue(
        registry.getCandidateGraphIds("corpuser", "nativeGroupMembership").contains("membership"));
    assertTrue(
        registry.getCandidateGraphIds("corpGroup", "nativeGroupMembership").contains("membership"));
    assertEquals(
        registry.getRelationshipTypesForAspect("membership", "nativeGroupMembership"),
        Set.of("IsMemberOfNativeGroup"));
  }

  /**
   * {@link EntityGraphRegistry#getGraphIdsForEntityType} and {@link
   * com.linkedin.metadata.graph.cache.service.invalidation.GraphCacheInvalidator#graphIdsForEntityType}
   * share the same resolved-edge scan on the registry.
   */
  @Test
  public void getGraphIdsForEntityTypeUsesResolvedEdgeScan() {
    EntityGraphRegistry registry = buildRegistryWithKnownGraphs();

    assertEquals(registry.getGraphIdsForEntityType("domain"), Set.of("domain"));
    assertTrue(registry.getGraphIdsForEntityType("dataset").isEmpty());
    assertEquals(
        registry.getGraphIdsForEntityType("glossaryNode"),
        graphIdsForEntityTypeFromResolvedEdges(registry, "glossaryNode"));
  }

  private static Set<String> graphIdsForEntityTypeFromResolvedEdges(
      EntityGraphRegistry registry, String entityType) {
    Set<String> graphIds = new java.util.HashSet<>();
    registry
        .getGraphsById()
        .values()
        .forEach(
            definition ->
                definition
                    .getResolvedEdges()
                    .forEach(
                        edge -> {
                          String source = edge.getTriplet().getSourceEntityType();
                          String destination = edge.getTriplet().getDestinationEntityType();
                          if (entityType.equals(source) || entityType.equals(destination)) {
                            graphIds.add(definition.getGraphId());
                          }
                        }));
    return graphIds;
  }

  @Test
  public void testZeroResolvedEdgesAllowedWhenCacheDisabled() {
    EntityGraphCacheProperties properties = new EntityGraphCacheProperties();
    properties.setEnabled(false);
    properties.setGraphs(new java.util.HashMap<>());
    properties.getGraphs().put("bad", invalidEntityGraph());

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    assertEquals(registry.getGraphsById().size(), 0);
  }

  @Test
  public void testOptionalBindingsStillIndexedWhenPresent() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties.getGraphs().get("domain").getBindings().getFilterFields().add("domains.keyword");

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    EntityGraphBinding filterBinding = registry.getBindingForFilterField("domains.keyword");
    assertNotNull(filterBinding);
    assertEquals(filterBinding.getGraphId(), "domain");
    assertNotNull(registry.resolveKnownGraph(KnownEntityGraph.DOMAIN));
  }

  @Test
  public void testPolicyFieldBindingsIndexed() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties
        .getGraphs()
        .get("domain")
        .getBindings()
        .setPolicyFieldTypes(new java.util.ArrayList<>());
    properties.getGraphs().get("domain").getBindings().getPolicyFieldTypes().add("DOMAIN");

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    EntityGraphBinding policyBinding = registry.getBindingForPolicyField("domain");
    assertNotNull(policyBinding);
    assertEquals(policyBinding.getGraphId(), "domain");
  }

  @Test
  public void testDuplicatePolicyFieldBindingsFailWhenEnabled() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties
        .getGraphs()
        .get("domain")
        .getBindings()
        .setPolicyFieldTypes(new java.util.ArrayList<>());
    properties.getGraphs().get("domain").getBindings().getPolicyFieldTypes().add("DOMAIN");
    GraphDefinition other = validDomainGraphDefinition();
    other.getBindings().setPolicyFieldTypes(new java.util.ArrayList<>());
    other.getBindings().getPolicyFieldTypes().add("DOMAIN");
    properties.getGraphs().put("other-domain", other);

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                EntityGraphRegistry.build(
                    properties,
                    new TestEntityRegistry(),
                    new LineageRegistry(new TestEntityRegistry())));
    assertTrue(ex.getMessage().contains("policy field type"));
  }

  @Test
  public void testExpandTripletEdgesFromEntityTypesShorthand() {
    GraphDefinition graph = new GraphDefinition();
    graph.setEntityTypes(List.of("domain", "glossaryNode"));
    graph.setRelationshipType("IsPartOf");

    List<com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet> triplets =
        EntityGraphRegistry.expandTripletEdges(graph);

    assertEquals(triplets.size(), 2);
    assertEquals(triplets.get(0).getSourceEntityType(), "domain");
    assertEquals(triplets.get(0).getRelationshipType(), "IsPartOf");
  }

  @Test
  public void testExpandLineageEdgesHonorsEntityTypeFilter() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    GraphDefinition graph = validDomainGraphDefinition();
    graph.setEdges(null);
    EntityGraphCacheProperties.LineageEdgeConfig lineage =
        new EntityGraphCacheProperties.LineageEdgeConfig();
    lineage.setEntityTypes(List.of("dataset"));
    graph.setLineage(lineage);
    properties.getGraphs().put("lineage-graph", graph);

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    assertNotNull(registry.getDefinition("lineage-graph"));
    assertFalse(registry.getDefinition("lineage-graph").getEdges().isEmpty());
  }

  @Test
  public void testHasFullScopeGraphsAndScheduledFullGraphs() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties.getGraphs().get("domain").getPopulation().setStrategy(PopulationStrategy.SCHEDULED);
    properties
        .getGraphs()
        .get("membership")
        .getPopulation()
        .setStrategy(PopulationStrategy.SCHEDULED);

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    assertTrue(registry.hasFullScopeGraphs());
    assertEquals(registry.getScheduledFullGraphs().size(), 2);
    assertEquals(registry.getScheduledFullGraphs().get(0).getGraphId(), "domain");
    assertEquals(registry.getScheduledFullGraphs().get(1).getGraphId(), "membership");
  }

  @Test
  public void testWrongScopeForKnownGraphFailsWhenEnabled() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties.getGraphs().get("glossary").getScope().setMode(ScopeMode.FULL);

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                EntityGraphRegistry.build(
                    properties,
                    new TestEntityRegistry(),
                    new LineageRegistry(new TestEntityRegistry())));
    assertTrue(ex.getMessage().contains("GLOSSARY"));
    assertTrue(ex.getMessage().contains("PARTIAL"));
  }

  @Test
  public void testFullPrimaryBuildSourceRejectedWhenEnabled() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    GraphDefinition graph = validPartialGraphDefinition();
    graph.getScope().setMode(ScopeMode.FULL);
    graph.setBuildSource("primary");
    properties.getGraphs().put("bad-full-primary", graph);

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                EntityGraphRegistry.build(
                    properties,
                    new TestEntityRegistry(),
                    new LineageRegistry(new TestEntityRegistry())));
    assertTrue(ex.getMessage().contains("FULL scope requires buildSource graph or search"));
  }

  @Test
  public void testPerGraphLocalEvictionOverridesGlobalDefaults() {
    EntityGraphCacheProperties properties = enabledPropertiesWithKnownGraphs();
    properties
        .getGraphs()
        .get("domain")
        .setEviction(
            EntityGraphCacheProperties.GraphEviction.builder()
                .local(
                    EntityGraphCacheProperties.LocalEviction.builder()
                        .enabled(true)
                        .maxViews(32)
                        .maxEstimatedBytes(536870912L)
                        .build())
                .build());

    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            properties, new TestEntityRegistry(), new LineageRegistry(new TestEntityRegistry()));

    assertEquals(registry.getDefinition("domain").getLocalEviction().getMaxViews(), 32);
    assertEquals(
        registry.getDefinition("domain").getLocalEviction().getMaxEstimatedBytes(), 536870912L);
  }

  private static GraphDefinition validPartialGraphDefinition() {
    GraphDefinition graph = validDomainGraphDefinition();
    graph.getScope().setMode(ScopeMode.PARTIAL);
    graph.getScope().setMaxDepth(15);
    graph.setBuildSource("primary");
    return graph;
  }

  private static EntityGraphRegistry buildRegistryWithKnownGraphs() {
    return EntityGraphRegistry.build(
        enabledPropertiesWithKnownGraphs(),
        new TestEntityRegistry(),
        new LineageRegistry(new TestEntityRegistry()));
  }

  private static EntityGraphCacheProperties enabledPropertiesWithKnownGraphs() {
    EntityGraphCacheProperties properties = new EntityGraphCacheProperties();
    properties.setEnabled(true);
    properties.setEviction(minimalEviction());
    properties.setGraphs(new java.util.HashMap<>());
    properties.getGraphs().put("domain", validDomainGraphDefinition());
    properties.getGraphs().put("glossary", validGlossaryGraphDefinition());
    properties.getGraphs().put("container", validContainerGraphDefinition());
    properties.getGraphs().put("membership", validMembershipGraphDefinition());
    return properties;
  }

  private static EntityGraphCacheProperties.Eviction minimalEviction() {
    return EntityGraphCacheProperties.Eviction.builder()
        .local(
            EntityGraphCacheProperties.LocalEviction.builder()
                .enabled(true)
                .maxViews(16)
                .maxEstimatedBytes(268435456L)
                .build())
        .build();
  }

  private static GraphDefinition validDomainGraphDefinition() {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource("search");
    graph.setScope(EntityGraphCacheProperties.ScopeConfig.builder().mode(ScopeMode.FULL).build());
    graph.setPopulation(
        GraphPopulation.builder().strategy(PopulationStrategy.LAZY).intervalSeconds(300).build());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("domain")
                .destinationEntityType("domain")
                .relationshipType("IsPartOf")
                .build()));
    EntityGraphCacheProperties.GraphBindings bindings =
        new EntityGraphCacheProperties.GraphBindings();
    bindings.setFilterFields(new java.util.ArrayList<>());
    bindings.setPolicyFieldTypes(new java.util.ArrayList<>());
    graph.setBindings(bindings);
    return graph;
  }

  private static GraphDefinition validGlossaryGraphDefinition() {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource("graph");
    graph.setScope(
        EntityGraphCacheProperties.ScopeConfig.builder()
            .mode(ScopeMode.PARTIAL)
            .maxDepth(15)
            .build());
    graph.setPopulation(
        GraphPopulation.builder().strategy(PopulationStrategy.LAZY).intervalSeconds(300).build());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("glossaryNode")
                .destinationEntityType("glossaryNode")
                .relationshipType("IsPartOf")
                .build(),
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("glossaryTerm")
                .destinationEntityType("glossaryNode")
                .relationshipType("IsPartOf")
                .build()));
    return graph;
  }

  private static GraphDefinition validContainerGraphDefinition() {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource("graph");
    graph.setScope(
        EntityGraphCacheProperties.ScopeConfig.builder()
            .mode(ScopeMode.PARTIAL)
            .maxDepth(12)
            .build());
    graph.setPopulation(
        GraphPopulation.builder().strategy(PopulationStrategy.LAZY).intervalSeconds(1200).build());
    graph.setBounds(
        EntityGraphCacheProperties.GraphBounds.builder().maxVertices(5000).maxEdges(7500).build());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("container")
                .destinationEntityType("container")
                .relationshipType("IsPartOf")
                .build()));
    return graph;
  }

  private static GraphDefinition validMembershipGraphDefinition() {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource("graph");
    graph.setScope(EntityGraphCacheProperties.ScopeConfig.builder().mode(ScopeMode.FULL).build());
    graph.setPopulation(
        GraphPopulation.builder().strategy(PopulationStrategy.LAZY).intervalSeconds(600).build());
    graph.setBounds(
        EntityGraphCacheProperties.GraphBounds.builder()
            .maxVertices(21000)
            .maxEdges(60000)
            .build());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("corpuser")
                .destinationEntityType("corpGroup")
                .relationshipType("IsMemberOfGroup")
                .build(),
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("corpuser")
                .destinationEntityType("corpGroup")
                .relationshipType("IsMemberOfNativeGroup")
                .build(),
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("corpuser")
                .destinationEntityType("dataHubRole")
                .relationshipType("IsMemberOfRole")
                .build(),
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("corpGroup")
                .destinationEntityType("dataHubRole")
                .relationshipType("IsMemberOfRole")
                .build()));
    return graph;
  }

  private static GraphDefinition validGraphDefinition(String buildSource) {
    GraphDefinition graph = new GraphDefinition();
    graph.setEnabled(true);
    graph.setBuildSource(buildSource);
    graph.setScope(
        EntityGraphCacheProperties.ScopeConfig.builder()
            .mode("search".equalsIgnoreCase(buildSource) ? ScopeMode.FULL : ScopeMode.PARTIAL)
            .build());
    graph.setPopulation(
        GraphPopulation.builder().strategy(PopulationStrategy.LAZY).intervalSeconds(300).build());
    graph.setEdges(
        List.of(
            EntityGraphCacheProperties.GraphEdgeTripletConfig.builder()
                .sourceEntityType("domain")
                .destinationEntityType("domain")
                .relationshipType("IsPartOf")
                .build()));
    return graph;
  }

  private static GraphDefinition invalidEntityGraph() {
    GraphDefinition graph = validDomainGraphDefinition();
    graph.getEdges().get(0).setRelationshipType("DefinitelyNotARealRelationship");
    return graph;
  }
}
