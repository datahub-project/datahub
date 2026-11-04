package com.linkedin.gms.factory.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.context.OperationFingerprint;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.Map;
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

  private static ApplicationContextRunner runner(SearchEngineType engineType) {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(engineType);
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            IndexConventionFactory.class,
            SettingsBuilderFactory.class,
            SingleClusterSearchTestConfiguration.class)
        .withBean("searchClientShim", SearchClientShim.class, () -> client);
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
                    .isInstanceOf(IllegalStateException.class));
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
}
