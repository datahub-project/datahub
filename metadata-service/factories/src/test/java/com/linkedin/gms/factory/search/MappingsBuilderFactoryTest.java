package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class MappingsBuilderFactoryTest {

  /** Non-default so a builder that fell back to {@link ESUtils#KEYWORD_MAXLENGTH} is caught. */
  private static final int CONFIGURED_KEYWORD_MAX_LENGTH = 1000;

  /**
   * The factory must hand the configured keyword limit to every {@code V2MappingsBuilder} it
   * builds. Tests that construct the builder directly cannot catch a factory that drops it.
   */
  @Test
  public void testFactoryWiresKeywordMaxLengthIntoV2Builders() throws Exception {
    EntityIndexConfiguration entityIndex =
        EntityIndexConfiguration.builder()
            .semanticSearch(new SemanticSearchConfiguration())
            .build();
    ElasticSearchConfiguration elasticSearch =
        ElasticSearchConfiguration.builder().entityIndex(entityIndex).build();

    ConfigurationProvider configProvider = new ConfigurationProvider();
    configProvider.setElasticSearch(elasticSearch);
    configProvider.setStructuredProperties(
        StructuredPropertiesConfiguration.builder()
            .keywordMaxLength(CONFIGURED_KEYWORD_MAX_LENGTH)
            .build());

    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.partialNgramConfig()).thenReturn(OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    SearchClusterRegistry registry =
        SearchClusterRegistry.singleCluster(
            elasticSearch, client, mock(ESBulkProcessor.class), mock(ESIndexBuilder.class));

    MappingsBuilderFactory factory = new MappingsBuilderFactory();
    MappingsBuilder legacy = factory.createLegacyMappingsBuilder(configProvider, registry);
    MappingsBuilder semantic =
        factory.createSemanticSearchMappingsBuilder(
            configProvider, legacy, IndexConventionImpl.noPrefix("MD5", entityIndex), registry);

    assertConfiguredIgnoreAbove(legacy);
    assertConfiguredIgnoreAbove(semantic);
  }

  private static void assertConfiguredIgnoreAbove(MappingsBuilder builder) throws Exception {
    StructuredPropertyDefinition stringProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("myStringProp")
            .setDisplayName("My String")
            .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappings =
        builder.getIndexMappingsForStructuredProperty(
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:myStringProp"), stringProp)));

    @SuppressWarnings("unchecked")
    Map<String, Object> stringMapping = (Map<String, Object>) mappings.get("myStringProp");
    int expected = ESUtils.keywordIgnoreAboveForMaxBytes(CONFIGURED_KEYWORD_MAX_LENGTH);
    assertNotEquals(
        expected,
        ESUtils.keywordIgnoreAboveForMaxBytes(ESUtils.KEYWORD_MAXLENGTH),
        "configured limit must differ from the default for this test to mean anything");
    assertEquals(
        stringMapping.get(ESUtils.IGNORE_ABOVE),
        expected,
        "STRING structured property ignore_above must come from structuredProperties.keywordMaxLength");
  }

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
