package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.HashMap;
import java.util.Map;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Unit tests for startup gates in {@link SearchClientShimFactory} (unsupported ES 7 engine type,
 * IAM credentials, nmslib on OpenSearch 3). ES 8.18+ semantic-search version checks live in {@code
 * Es8SearchClientShimVersionTest}.
 */
public class SearchClientShimFactorySemanticGateTest {

  @Test
  public void parseEngineTypeRejectsElasticsearch7() {
    SearchClientShimFactory factory = new SearchClientShimFactory();
    for (String alias : new String[] {"ELASTICSEARCH_7", "ES7"}) {
      IllegalArgumentException ex =
          expectThrows(
              IllegalArgumentException.class,
              () -> ReflectionTestUtils.invokeMethod(factory, "parseEngineType", alias));
      assertTrue(
          ex.getMessage().contains("no longer supported"),
          "Message should say Elasticsearch 7 is unsupported; got: " + ex.getMessage());
    }
  }

  @Test
  public void factoryRejectsOpenSearchIamAuthWithoutSharedCredentials() {
    SearchClusterSettings cluster =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .opensearchUseAwsIamAuth(true)
            .region("us-east-1")
            .build();

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                SearchClientShimFactory.assertIamAuthHasSharedCredentials(
                    "primary", cluster, null));
    assertTrue(ex.getMessage().contains("DefaultCredentialsProvider"));
  }

  @Test
  public void factoryAllowsOpenSearchIamAuthWithSharedCredentials() {
    SearchClusterSettings cluster =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .opensearchUseAwsIamAuth(true)
            .region("us-east-1")
            .build();
    SearchClientShimFactory.assertIamAuthHasSharedCredentials(
        "primary", cluster, mock(AwsCredentialsProvider.class));
  }

  @Test
  public void factoryRejectsNmslibModelOnOpenSearch3WhenSemanticSearchEnabled() {
    SearchClientShim<?> os3Shim = mock(SearchClientShim.class);
    when(os3Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () ->
                SearchClientShimFactory.assertNoNmslibOnOpenSearch3(
                    os3Shim, semanticConfiguration("nmslib"), true));
    assertTrue(
        ex.getMessage().contains("nmslib"), "Message should name nmslib: " + ex.getMessage());
  }

  @Test
  public void factoryAllowsNmslibOutsideOpenSearch3AndOtherEnginesOnOpenSearch3() {
    SearchClientShim<?> os2Shim = mock(SearchClientShim.class);
    when(os2Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    SearchClientShim<?> os3Shim = mock(SearchClientShim.class);
    when(os3Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);

    // Must not throw: nmslib is still valid on 2.x, faiss is valid on 3.x, and the gate is a
    // no-op when semantic search is disabled.
    SearchClientShimFactory.assertNoNmslibOnOpenSearch3(
        os2Shim, semanticConfiguration("nmslib"), true);
    SearchClientShimFactory.assertNoNmslibOnOpenSearch3(
        os3Shim, semanticConfiguration("faiss"), true);
    SearchClientShimFactory.assertNoNmslibOnOpenSearch3(
        os3Shim, semanticConfiguration("nmslib"), false);
  }

  @Test
  public void factoryNmslibGuardSkipsWhenNoModelsOrNoEngineConfigured() {
    SearchClientShim<?> os3Shim = mock(SearchClientShim.class);
    when(os3Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);

    ElasticSearchConfiguration noModels = semanticConfiguration("nmslib");
    noModels.getEntityIndex().getSemanticSearch().setModels(null);
    SearchClientShimFactory.assertNoNmslibOnOpenSearch3(os3Shim, noModels, true);

    Map<String, ModelEmbeddingConfig> sparse = new HashMap<>();
    sparse.put("absent", null);
    ModelEmbeddingConfig noEngine = new ModelEmbeddingConfig();
    noEngine.setKnnEngine(null);
    sparse.put("no_engine", noEngine);
    ElasticSearchConfiguration sparseModels = semanticConfiguration("faiss");
    sparseModels.getEntityIndex().getSemanticSearch().setModels(sparse);
    SearchClientShimFactory.assertNoNmslibOnOpenSearch3(os3Shim, sparseModels, true);
  }

  @Test
  public void parseEngineTypeAcceptsOpenSearch3Aliases() {
    SearchClientShimFactory factory = new SearchClientShimFactory();
    assertEquals(
        ReflectionTestUtils.invokeMethod(factory, "parseEngineType", "OPENSEARCH_3"),
        SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertEquals(
        ReflectionTestUtils.invokeMethod(factory, "parseEngineType", "os3"),
        SearchClientShim.SearchEngineType.OPENSEARCH_3);
    IllegalArgumentException unsupported =
        expectThrows(
            IllegalArgumentException.class,
            () -> ReflectionTestUtils.invokeMethod(factory, "parseEngineType", "OPENSEARCH_9"));
    assertTrue(unsupported.getMessage().contains("OPENSEARCH_3"), unsupported.getMessage());
  }

  private static ElasticSearchConfiguration semanticConfiguration(String knnEngine) {
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setKnnEngine(knnEngine);
    SemanticSearchConfiguration semantic = new SemanticSearchConfiguration();
    semantic.setEnabled(true);
    semantic.setModels(Map.of("test_model", model));
    EntityIndexConfiguration entityIndex = new EntityIndexConfiguration();
    entityIndex.setSemanticSearch(semantic);
    ElasticSearchConfiguration configuration = new ElasticSearchConfiguration();
    configuration.setEntityIndex(entityIndex);
    return configuration;
  }
}
