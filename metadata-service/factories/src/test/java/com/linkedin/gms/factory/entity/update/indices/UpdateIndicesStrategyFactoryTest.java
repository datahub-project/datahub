package com.linkedin.gms.factory.entity.update.indices;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.PartialNgramEntityRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class UpdateIndicesStrategyFactoryTest {

  /**
   * The V3 strategy's own mappings take the partial-ngram shape from the V3 search client, as the
   * mappings builder bean does, so the two cannot disagree on an engine such as ES8 that strips
   * {@code doc_values: false} from {@code search_as_you_type}.
   */
  @Test
  public void testV3StrategyMappingsUseV3ClientPartialNgramConfig() {
    List<Map<String, Object>> es8Partials =
        searchAsYouTypeFields(v3StrategyMappings(Es8SearchClientShim.PARTIAL_NGRAM_CONFIG));
    assertFalse(
        es8Partials.isEmpty(), "the TEXT_PARTIAL subfield should map to search_as_you_type");
    es8Partials.forEach(field -> assertFalse(field.containsKey("doc_values"), field.toString()));

    List<Map<String, Object>> openSearchPartials =
        searchAsYouTypeFields(v3StrategyMappings(OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG));
    assertFalse(openSearchPartials.isEmpty());
    openSearchPartials.forEach(field -> assertEquals(field.get("doc_values"), "false"));
  }

  private static Collection<IndexMapping> v3StrategyMappings(Map<String, String> ngramConfig) {
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v3Client.partialNgramConfig()).thenReturn(ngramConfig);
    SearchClusterRegistry searchClusterRegistry = mock(SearchClusterRegistry.class);
    doReturn(v3Client).when(searchClusterRegistry).clientFor(SearchComponent.SEARCH_V3);

    UpdateIndicesStrategy strategy =
        new UpdateIndicesStrategyFactory()
            .createUpdateIndicesV3Strategy(
                mock(ElasticSearchService.class),
                mock(SearchDocumentTransformer.class),
                mock(TimeseriesAspectService.class),
                null,
                mock(ConfigurationProvider.class),
                new Sha256UrnEntityDocumentIdHasher(),
                List.of(),
                false,
                false,
                "MD5",
                "ebean",
                searchClusterRegistry,
                null,
                mock(StructuredPropertyMappingCache.class),
                List.of());

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(new PartialNgramEntityRegistry());
    return strategy.getIndexMappings(opContext);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> searchAsYouTypeFields(Object node) {
    List<Map<String, Object>> found = new ArrayList<>();
    if (node instanceof IndexMapping mapping) {
      found.addAll(searchAsYouTypeFields(mapping.getMappings()));
    } else if (node instanceof Map<?, ?> map) {
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
