package com.linkedin.gms.factory.search;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.entity.update.indices.UpdateIndicesStrategyFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.Map;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.springframework.core.env.StandardEnvironment;
import org.testng.annotations.Test;

public class SearchV3ReadHoldTest {

  @Test
  public void testNoV2IndexLeavesTheFlagsAlone() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    StandardEnvironment environment = new StandardEnvironment();
    hold(environment, false, null).applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertFalse(environment.getPropertySources().contains(SearchV3ReadHold.PROPERTY_SOURCE));
    assertFalse(UpdateIndicesStrategyFactory.liveV2Enabled(config));
  }

  @Test
  public void testIncompletePairHoldsReadsAndPublishesTheFlags() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    StandardEnvironment environment = new StandardEnvironment();
    hold(environment, true, Map.of()).applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertFalse(config.getEntityIndex().getV3().isHybridReadEnabled());
    assertEquals(environment.getProperty(SearchV3ReadHold.V2_ENABLED), "true");
    assertEquals(environment.getProperty(SearchV3ReadHold.V3_KEYWORD_READ), "false");
    assertEquals(environment.getProperty(SearchV3ReadHold.V3_HYBRID_READ), "false");
    assertTrue(UpdateIndicesStrategyFactory.liveV2Enabled(config));
  }

  @Test
  public void testCompletePairDoesNotHold() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    hold(null, true, Map.of("_meta", Map.of("migration", "complete"))).applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertFalse(UpdateIndicesStrategyFactory.liveV2Enabled(config));
  }

  @Test
  public void testV2ListingFailureStopsStartup() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    SearchClientShim<?> failing = mock(SearchClientShim.class);
    when(failing.indexExists(any(), any(), any())).thenThrow(new IOException("down"));
    doReturn(failing).when(registry).clientFor(SearchComponent.SEARCH_V2);
    doReturn(mock(SearchClientShim.class)).when(registry).clientFor(SearchComponent.SEARCH_V3);
    SearchV3ReadHold failingHold =
        new SearchV3ReadHold(registry, indexConvention(), entityRegistry(), null);

    assertThrows(IllegalStateException.class, () -> failingHold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
  }

  @Test
  public void testNullV2BlockIsCreatedAndHeld() throws Exception {
    ElasticSearchConfiguration config =
        ElasticSearchConfiguration.builder()
            .entityIndex(
                EntityIndexConfiguration.builder()
                    .v3(
                        EntityIndexVersionConfiguration.builder()
                            .enabled(true)
                            .keywordReadEnabled(true)
                            .hybridReadEnabled(true)
                            .build())
                    .build())
            .build();
    hold(new StandardEnvironment(), true, Map.of()).applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  private static ElasticSearchConfiguration v3Reads() {
    return ElasticSearchConfiguration.builder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(true)
                        .hybridReadEnabled(true)
                        .build())
                .build())
        .build();
  }

  private static SearchV3ReadHold hold(
      StandardEnvironment environment, boolean v2Exists, Map<String, Object> v3Mapping)
      throws Exception {
    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    SearchClientShim<?> v2Client = mock(SearchClientShim.class);
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v2Client.indexExists(any(), any(), any())).thenReturn(v2Exists);
    when(v3Client.indexExists(any(), any(), any())).thenReturn(v2Exists);
    GetMappingsResponse mappings = mock(GetMappingsResponse.class);
    MappingMetadata metadata = mock(MappingMetadata.class);
    when(metadata.getSourceAsMap()).thenReturn(v3Mapping);
    when(mappings.mappings()).thenReturn(Map.of("datasetindex_v3", metadata));
    when(v3Client.getIndexMapping(any(), any(), any())).thenReturn(mappings);
    doReturn(v2Client).when(registry).clientFor(SearchComponent.SEARCH_V2);
    doReturn(v3Client).when(registry).clientFor(SearchComponent.SEARCH_V3);
    return new SearchV3ReadHold(registry, indexConvention(), entityRegistry(), environment);
  }

  private static IndexConvention indexConvention() {
    IndexConvention convention = mock(IndexConvention.class);
    when(convention.getEntityIndexName(any(), any())).thenReturn("datasetindex_v2");
    when(convention.getEntityIndexNameV3(any(), any())).thenReturn("datasetindex_v3");
    return convention;
  }

  private static EntityRegistry entityRegistry() {
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getName()).thenReturn("dataset");
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getEntitySpecs()).thenReturn(Map.of("dataset", spec));
    return registry;
  }
}
