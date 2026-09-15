package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ComponentClusterConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.testng.annotations.Test;

public class SearchClusterRegistryTest {

  private static final IndexConvention CONVENTION =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());

  private static ElasticSearchConfiguration config(ComponentClusterConfiguration routing) {
    Map<String, SearchClusterSettings> clusters = new LinkedHashMap<>();
    clusters.put("primary", SearchClusterSettings.builder().uri("http://search:9200").build());
    clusters.put("secondary", SearchClusterSettings.builder().uri("http://search2:9200").build());
    return ElasticSearchConfiguration.builder()
        .clusters(clusters)
        .componentCluster(routing)
        .build();
  }

  private static SearchClusterRegistry registry(
      ElasticSearchConfiguration config, ESIndexBuilder primary, ESIndexBuilder secondary) {
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection("primary", config, null, null, primary));
    connections.put(
        "secondary",
        new SearchClusterRegistry.ClusterConnection("secondary", config, null, null, secondary));
    return new SearchClusterRegistry(config, connections);
  }

  @Test
  public void testEntityIndexNamesMapToTheirComponent() {
    assertEquals(
        SearchClusterRegistry.componentForEntityIndex(CONVENTION, "datasetindex_v2"),
        SearchComponent.SEARCH_V2);
    assertEquals(
        SearchClusterRegistry.componentForEntityIndex(CONVENTION, "datasetindex_v3"),
        SearchComponent.SEARCH_V3);
    assertEquals(
        SearchClusterRegistry.componentForEntityIndex(CONVENTION, "datasetindex_v2_semantic"),
        SearchComponent.SEMANTIC);
  }

  @Test
  public void testZeroDowntimeBackingIndexRoutesWithItsFamily() {
    assertEquals(
        SearchClusterRegistry.componentForEntityIndex(CONVENTION, "datasetindex_v3_1712345678"),
        SearchComponent.SEARCH_V3);
    assertEquals(
        SearchClusterRegistry.componentForEntityIndex(
            CONVENTION, "datasetindex_v2_semantic_1712345678"),
        SearchComponent.SEMANTIC);
    assertEquals(
        SearchClusterRegistry.componentForEntityIndex(CONVENTION, "datasetindex_v2_next_123"),
        SearchComponent.SEARCH_V2);
  }

  @Test
  public void testAllComponentsOnPrimaryNeedsNoResolver() {
    ElasticSearchConfiguration config = config(ComponentClusterConfiguration.builder().build());
    SearchClusterRegistry reg =
        registry(config, mock(ESIndexBuilder.class), mock(ESIndexBuilder.class));

    // Single-cluster deployments keep the existing single-builder behavior untouched.
    assertNull(reg.entityIndexBuilderResolver(CONVENTION));
    assertTrue(reg.sameCluster(SearchComponent.SEARCH_V2, SearchComponent.SEARCH_V3));
  }

  @Test
  public void testSplitSearchVersionsResolveToDifferentBuilders() {
    ElasticSearchConfiguration config =
        config(ComponentClusterConfiguration.builder().searchV3("secondary").build());
    ESIndexBuilder primary = mock(ESIndexBuilder.class);
    ESIndexBuilder secondary = mock(ESIndexBuilder.class);
    SearchClusterRegistry reg = registry(config, primary, secondary);

    Function<String, ESIndexBuilder> resolver = reg.entityIndexBuilderResolver(CONVENTION);
    assertNotNull(resolver);
    assertSame(resolver.apply("datasetindex_v2"), primary);
    assertSame(resolver.apply("datasetindex_v3"), secondary);
    assertSame(resolver.apply("datasetindex_v3_1712345678"), secondary);
    assertThrows(IllegalArgumentException.class, () -> resolver.apply("graph_service_v1"));
    assertFalse(reg.sameCluster(SearchComponent.SEARCH_V2, SearchComponent.SEARCH_V3));
  }

  @Test
  public void testAllEntityFamiliesOnSecondaryUseSecondaryBuilder() {
    ElasticSearchConfiguration config =
        config(
            ComponentClusterConfiguration.builder()
                .searchV2("secondary")
                .searchV3("secondary")
                .semantic("secondary")
                .build());
    ESIndexBuilder primary = mock(ESIndexBuilder.class);
    ESIndexBuilder secondary = mock(ESIndexBuilder.class);
    SearchClusterRegistry reg = registry(config, primary, secondary);

    Function<String, ESIndexBuilder> resolver = reg.entityIndexBuilderResolver(CONVENTION);
    assertNotNull(resolver);
    assertSame(resolver.apply("datasetindex_v2"), secondary);
    assertSame(resolver.apply("datasetindex_v3"), secondary);
    assertSame(resolver.apply("datasetindex_v2_semantic"), secondary);
  }

  @Test
  public void testSemanticOnSecondaryRoutesOnlySemantic() {
    ElasticSearchConfiguration config =
        config(ComponentClusterConfiguration.builder().semantic("secondary").build());
    ESIndexBuilder primary = mock(ESIndexBuilder.class);
    ESIndexBuilder secondary = mock(ESIndexBuilder.class);
    SearchClusterRegistry reg = registry(config, primary, secondary);

    Function<String, ESIndexBuilder> resolver = reg.entityIndexBuilderResolver(CONVENTION);
    assertNotNull(resolver);
    assertSame(resolver.apply("datasetindex_v2_semantic"), secondary);
    assertSame(resolver.apply("datasetindex_v2_semantic_1712345678"), secondary);
    assertSame(resolver.apply("datasetindex_v2"), primary);
    assertSame(resolver.apply("datasetindex_v3"), primary);
  }

  @Test
  public void testGraphIndexIsNotClassifiedAsSearchV2() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchClusterRegistry.componentForEntityIndex(CONVENTION, "graph_service_v1"));
  }

  @Test
  public void testUnknownClusterNameFails() {
    ElasticSearchConfiguration config = config(ComponentClusterConfiguration.builder().build());
    SearchClusterRegistry reg =
        registry(config, mock(ESIndexBuilder.class), mock(ESIndexBuilder.class));

    assertThrows(IllegalArgumentException.class, () -> reg.connection("tertiary"));
  }

  @Test
  public void testRoutedClusterWithoutClientFailsAtConstruction() {
    ElasticSearchConfiguration config =
        config(ComponentClusterConfiguration.builder().usage("secondary").build());
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection(
            "primary", config, mock(SearchClientShim.class), null, mock(ESIndexBuilder.class)));

    assertThrows(IllegalStateException.class, () -> new SearchClusterRegistry(config, connections));
  }

  @Test
  public void testUniqueConnectionsDedupesSharedClients() {
    ElasticSearchConfiguration config = config(ComponentClusterConfiguration.builder().build());
    SearchClientShim<?> shared = mock(SearchClientShim.class);
    SearchClientShim<?> other = mock(SearchClientShim.class);
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection(
            "primary", config, shared, null, mock(ESIndexBuilder.class)));
    connections.put(
        "alias",
        new SearchClusterRegistry.ClusterConnection(
            "alias", config, shared, null, mock(ESIndexBuilder.class)));
    connections.put(
        "secondary",
        new SearchClusterRegistry.ClusterConnection(
            "secondary", config, other, null, mock(ESIndexBuilder.class)));

    SearchClusterRegistry reg = new SearchClusterRegistry(config, connections);

    List<SearchClusterRegistry.ClusterConnection> unique = List.copyOf(reg.uniqueConnections());
    assertEquals(unique.size(), 2);
    assertEquals(unique.get(0).getName(), "primary");
    assertEquals(unique.get(1).getName(), "secondary");
    assertSame(unique.get(0).getClient(), shared);
    assertSame(unique.get(1).getClient(), other);
  }
}
