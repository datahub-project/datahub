package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.PartialNgramEntityRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class MappingsBuilderFactoryTest {

  /** Search V3 does not support autocomplete, so its mappings have no autocomplete field. */
  @Test
  public void testV3MappingsHaveNoAutocompleteField() {
    Map<String, Object> properties =
        datasetV3Properties(
            v3ClientWithNgramConfig(OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG));
    assertNotNull(properties.get("name"));
    assertNull(properties.get("autocomplete"));
    assertNull(properties.get("_search"));
  }

  /**
   * V3 partial-ngram fields take their shape from the V3 search client. ES8 strips {@code
   * doc_values: false} from {@code search_as_you_type}, so authoring it there would never match the
   * mapping the cluster returns and every system update would see a mapping diff.
   */
  @Test
  public void testV3PartialNgramFieldsUseV3ClientConfig() {
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(new PartialNgramEntityRegistry());

    List<Map<String, Object>> es8Partials =
        searchAsYouTypeFields(
            v3Mappings(
                v3ClientWithNgramConfig(Es8SearchClientShim.PARTIAL_NGRAM_CONFIG), opContext));
    assertFalse(
        es8Partials.isEmpty(), "the TEXT_PARTIAL subfield should map to search_as_you_type");
    es8Partials.forEach(field -> assertFalse(field.containsKey("doc_values"), field.toString()));

    List<Map<String, Object>> openSearchPartials =
        searchAsYouTypeFields(
            v3Mappings(
                v3ClientWithNgramConfig(OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG),
                opContext));
    assertFalse(openSearchPartials.isEmpty());
    openSearchPartials.forEach(field -> assertEquals(field.get("doc_values"), "false"));
  }

  private static SearchClientShim<?> v3ClientWithNgramConfig(Map<String, String> ngramConfig) {
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v3Client.partialNgramConfig()).thenReturn(ngramConfig);
    return v3Client;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> datasetV3Properties(SearchClientShim<?> v3Client) {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    return (Map<String, Object>)
        v3IndexMappings(v3Client, opContext).stream()
            .filter(mapping -> mapping.getIndexName().endsWith("datasetindex_v3"))
            .findFirst()
            .orElseThrow()
            .getMappings()
            .get("properties");
  }

  private static List<Map<String, Object>> v3Mappings(
      SearchClientShim<?> v3Client, OperationContext opContext) {
    return v3IndexMappings(v3Client, opContext).stream()
        .map(IndexMapping::getMappings)
        .collect(Collectors.toList());
  }

  private static Collection<IndexMapping> v3IndexMappings(
      SearchClientShim<?> v3Client, OperationContext opContext) {
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
    return mappingsBuilder.getIndexMappings(opContext);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> searchAsYouTypeFields(Object node) {
    List<Map<String, Object>> found = new ArrayList<>();
    if (node instanceof Map<?, ?> map) {
      if ("search_as_you_type".equals(map.get("type"))) {
        found.add((Map<String, Object>) map);
      }
      map.values().forEach(child -> found.addAll(searchAsYouTypeFields(child)));
    } else if (node instanceof Collection<?> children) {
      children.forEach(child -> found.addAll(searchAsYouTypeFields(child)));
    }
    return found;
  }
}
