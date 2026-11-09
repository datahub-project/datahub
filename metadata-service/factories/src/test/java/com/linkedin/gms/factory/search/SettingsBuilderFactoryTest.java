package com.linkedin.gms.factory.search;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.RICH_TEXT_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.RICH_TEXT_DELIMITER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.RICH_TEXT_SEARCH_ANALYZER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Loads {@link SettingsBuilderFactory} with the production {@code application.yaml} and checks that
 * the {@code elasticsearch.entityIndex.v2/v3/semanticSearch.enabled} switches decide which index
 * families the {@code settingsBuilder} produces settings for. The search client is the only mock.
 */
public class SettingsBuilderFactoryTest {

  private static final String V2_ENABLED = "elasticsearch.entityIndex.v2.enabled";
  private static final String V3_ENABLED = "elasticsearch.entityIndex.v3.enabled";
  private static final String SEMANTIC_ENABLED = "elasticsearch.entityIndex.semanticSearch.enabled";
  private static final String DATASET_INDEX = "datasetindex_v2";
  private static final String DOCUMENT_INDEX = "documentindex_v2";

  private static ApplicationContextRunner runner(SearchEngineType engineType) {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(engineType);
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            IndexConventionFactory.class,
            SettingsBuilderFactory.class,
            SingleClusterSearchTestConfiguration.class)
        .withBean("searchClientShim", SearchClientShim.class, () -> client)
        .withBean(
            "entityRegistry", EntityRegistry.class, TestOperationContexts::defaultEntityRegistry);
  }

  @Test
  public void testProductionDefaultsBuildOnlyV2Settings() {
    runner(SearchEngineType.ELASTICSEARCH_8)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(settingsFor(context, v2Index(context))).containsKey("analysis");
              assertThat(settingsFor(context, v3Index(context))).isEmpty();
              assertThat(settingsFor(context, semanticIndex(context))).isEmpty();
            });
  }

  @Test
  public void testV3OnlyBuildsOnlyV3Settings() {
    runner(SearchEngineType.ELASTICSEARCH_8)
        .withPropertyValues(V2_ENABLED + "=false", V3_ENABLED + "=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(settingsFor(context, v2Index(context))).isEmpty();
              assertThat(settingsFor(context, v3Index(context)))
                  .containsEntry("mapping.total_fields.limit", 5000);
            });
  }

  @Test
  public void testBothGenerationsBuildTheirOwnSettings() {
    runner(SearchEngineType.ELASTICSEARCH_8)
        .withPropertyValues(V3_ENABLED + "=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(settingsFor(context, v2Index(context))).containsKey("analysis");
              assertThat(settingsFor(context, v3Index(context)))
                  .containsEntry("mapping.total_fields.limit", 5000);
            });
  }

  @Test
  public void testNeitherGenerationLeavesEmptySettings() {
    runner(SearchEngineType.ELASTICSEARCH_8)
        .withPropertyValues(V2_ENABLED + "=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(settingsFor(context, v2Index(context))).isEmpty();
              assertThat(settingsFor(context, v3Index(context))).isEmpty();
            });
  }

  @Test
  public void testSemanticSearchAddsKnnSettingsForSemanticIndices() {
    runner(SearchEngineType.OPENSEARCH_2)
        .withPropertyValues(SEMANTIC_ENABLED + "=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(settingsFor(context, semanticIndex(context))).containsEntry("knn", true);
              assertThat(settingsFor(context, v2Index(context)))
                  .containsKey("analysis")
                  .doesNotContainKey("knn");
            });
  }

  @Test
  public void testSemanticSearchWithoutV2FailsStartup() {
    runner(SearchEngineType.OPENSEARCH_2)
        .withPropertyValues(V2_ENABLED + "=false", SEMANTIC_ENABLED + "=true")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("elasticsearch.entityIndex.v2.enabled"));
  }

  private static Map<String, Object> settingsFor(ApplicationContext context, String indexName) {
    IndexConfiguration index =
        context.getBean(ConfigurationProvider.class).getElasticSearch().getIndex();
    return context.getBean("settingsBuilder", SettingsBuilder.class).getSettings(index, indexName);
  }

  private static IndexConvention convention(ApplicationContext context) {
    return context.getBean(IndexConventionFactory.INDEX_CONVENTION_BEAN, IndexConvention.class);
  }

  private static String v2Index(ApplicationContext context) {
    return convention(context).getEntityIndexName(OperationFingerprint.EMPTY, "dataset");
  }

  private static String v3Index(ApplicationContext context) {
    return convention(context).getEntityIndexNameV3(OperationFingerprint.EMPTY, "primary");
  }

  private static String semanticIndex(ApplicationContext context) {
    return convention(context).getEntityIndexNameSemantic(OperationFingerprint.EMPTY, "document");
  }

  /** Unit test of the V2 settings builder the factory creates, without a Spring context. */
  @Test
  public void testV2AnalysisForNonDocumentIndexOmitsRichTextAnalyzers() {
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();
    SettingsBuilder builder =
        new SettingsBuilderFactory()
            .createLegacySettingsBuilder(
                configurationProvider(),
                indexConvention(),
                searchClusterRegistry(indexConfiguration),
                entityRegistry());

    Map<String, Object> datasetAnalysis = analysis(builder, indexConfiguration, DATASET_INDEX);
    assertFalse(analyzers(datasetAnalysis).containsKey(RICH_TEXT_ANALYZER));
    assertFalse(analyzers(datasetAnalysis).containsKey(RICH_TEXT_SEARCH_ANALYZER));
    assertFalse(filters(datasetAnalysis).containsKey(RICH_TEXT_DELIMITER));

    Map<String, Object> documentAnalysis = analysis(builder, indexConfiguration, DOCUMENT_INDEX);
    assertTrue(analyzers(documentAnalysis).containsKey(RICH_TEXT_ANALYZER));
    assertTrue(analyzers(documentAnalysis).containsKey(RICH_TEXT_SEARCH_ANALYZER));
    assertTrue(filters(documentAnalysis).containsKey(RICH_TEXT_DELIMITER));
  }

  private static ConfigurationProvider configurationProvider() {
    SearchConfiguration search = SearchConfiguration.builder().enableSearchV2_5(false).build();
    ElasticSearchConfiguration elasticSearch =
        ElasticSearchConfiguration.builder().search(search).build();
    ConfigurationProvider configProvider = new ConfigurationProvider();
    configProvider.setElasticSearch(elasticSearch);
    return configProvider;
  }

  private static IndexConvention indexConvention() {
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndexType(any())).thenReturn(true);
    when(indexConvention.getEntityName(any(), eq(DATASET_INDEX)))
        .thenReturn(Optional.of("dataset"));
    when(indexConvention.getEntityName(any(), eq(DOCUMENT_INDEX)))
        .thenReturn(Optional.of("document"));
    return indexConvention;
  }

  private static SearchClusterRegistry searchClusterRegistry(
      IndexConfiguration indexConfiguration) {
    ElasticSearchConfiguration clusterConfig =
        ElasticSearchConfiguration.builder().index(indexConfiguration).build();
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    when(registry.configFor(SearchComponent.SEARCH_V2)).thenReturn(clusterConfig);
    doReturn(client).when(registry).clientFor(SearchComponent.SEARCH_V2);
    return registry;
  }

  private static EntityRegistry entityRegistry() {
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getSearchableFieldSpecs()).thenReturn(List.of());

    SearchableAnnotation richText = mock(SearchableAnnotation.class);
    when(richText.getFieldType()).thenReturn(SearchableAnnotation.FieldType.RICH_TEXT);
    SearchableFieldSpec richTextField = mock(SearchableFieldSpec.class);
    when(richTextField.getSearchableAnnotation()).thenReturn(richText);
    EntitySpec document = mock(EntitySpec.class);
    when(document.getSearchableFieldSpecs()).thenReturn(List.of(richTextField));

    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    when(entityRegistry.getEntitySpec("dataset")).thenReturn(dataset);
    when(entityRegistry.getEntitySpec("document")).thenReturn(document);
    return entityRegistry;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> analysis(
      SettingsBuilder builder, IndexConfiguration indexConfiguration, String indexName) {
    return (Map<String, Object>) builder.getSettings(indexConfiguration, indexName).get("analysis");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> analyzers(Map<String, Object> analysis) {
    return (Map<String, Object>) analysis.get("analyzer");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> filters(Map<String, Object> analysis) {
    return (Map<String, Object>) analysis.get("filter");
  }
}
