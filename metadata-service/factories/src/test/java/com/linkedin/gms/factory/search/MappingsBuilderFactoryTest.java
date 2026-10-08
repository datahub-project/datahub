package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import org.testng.annotations.Test;

public class MappingsBuilderFactoryTest {

  /**
   * Search V3 mappings take the ngram settings of the engine that hosts the V3 indices, on the
   * shared autocomplete field, the only ngram field of a V3 index.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testV3MappingsUseTheV3EngineNgramConfig() {
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v3Client.partialNgramConfig())
        .thenReturn(
            Map.of("type", "search_as_you_type", "max_shingle_size", "3", "doc_values", "false"));
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
            .createMultiEntityMappingsBuilder(configProvider, searchClusterRegistry, null);

    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexMapping datasets =
        mappingsBuilder.getIndexMappings(opContext).stream()
            .filter(mapping -> mapping.getIndexName().endsWith("datasetindex_v3"))
            .findFirst()
            .orElseThrow();
    Map<String, Object> search =
        (Map<String, Object>)
            ((Map<String, Object>) datasets.getMappings().get("properties")).get("_search");
    Map<String, Object> autocomplete =
        (Map<String, Object>) ((Map<String, Object>) search.get("properties")).get("autocomplete");
    Map<String, Object> ngram =
        (Map<String, Object>) ((Map<String, Object>) autocomplete.get("fields")).get("ngram");
    assertEquals(ngram.get("max_shingle_size"), "3");
  }
}
