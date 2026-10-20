package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
import java.util.Map;
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
}
