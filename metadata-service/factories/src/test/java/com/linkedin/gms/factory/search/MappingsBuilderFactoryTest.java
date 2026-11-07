package com.linkedin.gms.factory.search;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class MappingsBuilderFactoryTest {

  /** Search V3 does not support autocomplete, so its mappings have no autocomplete field. */
  @Test
  @SuppressWarnings("unchecked")
  public void testV3MappingsHaveNoAutocompleteField() {
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    SearchClusterRegistry searchClusterRegistry = mock(SearchClusterRegistry.class);
    doReturn(v3Client).when(searchClusterRegistry).clientFor(SearchComponent.SEARCH_V3);
    when(searchClusterRegistry.configFor(SearchComponent.SEARCH_V3))
        .thenReturn(
            ElasticSearchConfiguration.builder()
                .entityIndex(
                    EntityIndexConfiguration.builder()
                        .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
                        .build())
                .build());
    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    when(configProvider.getStructuredProperties())
        .thenReturn(StructuredPropertiesConfiguration.builder().keywordMaxLength(512).build());

    MappingsBuilder mappingsBuilder =
        new MappingsBuilderFactory()
            .createMultiEntityMappingsBuilder(
                configProvider,
                searchClusterRegistry,
                null,
                new V3DocumentIdScheme(new Sha256UrnEntityDocumentIdHasher()));

    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexMapping datasets =
        mappingsBuilder.getIndexMappings(opContext).stream()
            .filter(mapping -> mapping.getIndexName().endsWith("datasetindex_v3"))
            .findFirst()
            .orElseThrow();
    Map<String, Object> properties = (Map<String, Object>) datasets.getMappings().get("properties");
    assertNotNull(properties.get("name"));
    assertNull(properties.get("autocomplete"));
    assertNull(properties.get("_search"));
  }

  /**
   * The legacy and semantic beans exist whenever V3 is on. A fresh install leaves them out of the
   * index build, and a hold that flips the same V2 flag includes them.
   */
  @Test
  public void testV3DefaultOmitsV2MappingsUntilTheLiveFlagIsOn() {
    MappingsBuilder legacy = mappingNamed("datasetindex_v2");
    MappingsBuilder v3 = mappingNamed("datasetindex_v3");
    MappingsBuilder semantic = mappingNamed("datasetindex_v2_semantic");
    ElasticSearchConfiguration config =
        ElasticSearchConfiguration.builder()
            .entityIndex(
                EntityIndexConfiguration.builder()
                    .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
                    .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
                    .build())
            .build();

    MappingsBuilderFactory factory = new MappingsBuilderFactory();
    assertEquals(
        indexNames(factory.getInstance(config, legacy, v3, semantic)), Set.of("datasetindex_v3"));

    config.getEntityIndex().getV2().setEnabled(true);
    assertEquals(
        indexNames(factory.getInstance(config, legacy, v3, semantic)),
        Set.of("datasetindex_v2", "datasetindex_v3", "datasetindex_v2_semantic"));
  }

  private static MappingsBuilder mappingNamed(String indexName) {
    MappingsBuilder builder = mock(MappingsBuilder.class);
    IndexMapping mapping = IndexMapping.builder().indexName(indexName).mappings(Map.of()).build();
    doReturn(List.of(mapping)).when(builder).getIndexMappings(any(OperationContext.class), any());
    return builder;
  }

  private static Set<String> indexNames(MappingsBuilder builder) {
    return builder
        .getIndexMappings(TestOperationContexts.systemContextNoSearchAuthorization())
        .stream()
        .map(IndexMapping::getIndexName)
        .collect(Collectors.toSet());
  }
}
