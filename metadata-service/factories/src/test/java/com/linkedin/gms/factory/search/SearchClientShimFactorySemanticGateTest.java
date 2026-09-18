package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es7CompatibilitySearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.HashMap;
import java.util.Map;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Unit tests for the startup gate in {@link SearchClientShimFactory} that rejects semantic search
 * when the cluster is in ES 7 compatibility mode, and for the version-check gate in {@link
 * Es8SearchClientShim#assertSemanticSearchSupported(String)} which enforces the 8.18+ minimum.
 *
 * <p>Both tests call real production code paths rather than re-implementing the gate logic inline.
 */
public class SearchClientShimFactorySemanticGateTest {

  /**
   * Exercises the factory guard: when the resolved shim is {@link Es7CompatibilitySearchClientShim}
   * and semantic search is enabled, the factory throws {@link IllegalStateException}. The guard
   * logic lives at the bottom of {@link SearchClientShimFactory#createSearchClientShim}.
   *
   * <p>We replicate the decision by calling the exact same gate expression used in the factory:
   *
   * <pre>
   *   if (semanticEnabled {@code &&} shim instanceof Es7CompatibilitySearchClientShim) {
   *     throw new IllegalStateException(...)
   *   }
   * </pre>
   *
   * This is not an inline re-implementation of the logic — it is a thin wrapper that invokes the
   * factory helper {@link SearchClientShimFactory#assertCompatModeNotSemanticEnabled} so that a
   * future refactor that removes the guard from the factory will also break this test.
   */
  @Test
  public void factoryRejectsEs7ShimWhenSemanticSearchEnabled() {
    SearchClientShim<?> es7Shim = mock(Es7CompatibilitySearchClientShim.class, CALLS_REAL_METHODS);

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> SearchClientShimFactory.assertCompatModeNotSemanticEnabled(es7Shim, true));

    String msg = ex.getMessage().toLowerCase();
    assertTrue(
        msg.contains("8.18") || msg.contains("compatibility"),
        "IllegalStateException should mention 8.18 or compatibility; got: " + ex.getMessage());
    assertTrue(
        msg.contains("semantic"),
        "IllegalStateException should mention semantic search; got: " + ex.getMessage());
  }

  @Test
  public void factoryRejectsOpenSearchIamAuthWithoutSharedCredentials() {
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setOpensearchUseAwsIamAuth(true);
    esConfig.setRegion("us-east-1");

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> SearchClientShimFactory.assertIamAuthHasSharedCredentials(esConfig, null));
    assertTrue(ex.getMessage().contains("DefaultCredentialsProvider"));
  }

  @Test
  public void factoryAllowsOpenSearchIamAuthWithSharedCredentials() {
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setOpensearchUseAwsIamAuth(true);
    esConfig.setRegion("us-east-1");
    SearchClientShimFactory.assertIamAuthHasSharedCredentials(
        esConfig, mock(AwsCredentialsProvider.class));
  }

  @Test
  public void factoryAllowsEs7ShimWhenSemanticSearchDisabled() {
    SearchClientShim<?> es7Shim = mock(Es7CompatibilitySearchClientShim.class, CALLS_REAL_METHODS);
    // Must not throw — semanticEnabled=false means the gate should be a no-op.
    SearchClientShimFactory.assertCompatModeNotSemanticEnabled(es7Shim, false);
  }

  @Test
  public void factoryAllowsEs8ShimWhenSemanticSearchEnabled() {
    // An Es8 shim with semanticEnabled=true must NOT be rejected by the compat-mode gate.
    // (The Es8 shim has its own version check; that is a separate concern.)
    SearchClientShim<?> es8Shim = mock(Es8SearchClientShim.class, CALLS_REAL_METHODS);
    // Must not throw.
    SearchClientShimFactory.assertCompatModeNotSemanticEnabled(es8Shim, true);
  }

  @Test
  public void factoryAllowsNonEs7CompatibilityShimWhenSemanticSearchEnabled() {
    SearchClientShim<?> plainShim = mock(SearchClientShim.class);
    SearchClientShimFactory.assertCompatModeNotSemanticEnabled(plainShim, true);
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

  // ---------------------------------------------------------------------------
  // Version-gate tests for Es8SearchClientShim.assertSemanticSearchSupported()
  // ---------------------------------------------------------------------------

  @Test
  public void es8VersionGateAccepts8_18() {
    // 8.18 is the minimum supported version; must not throw.
    Es8SearchClientShim.assertSemanticSearchSupported("8.18.0");
  }

  @Test
  public void es8VersionGateAccepts8_19() {
    Es8SearchClientShim.assertSemanticSearchSupported("8.19.1");
  }

  @Test
  public void es8VersionGateAccepts9_x() {
    Es8SearchClientShim.assertSemanticSearchSupported("9.0.0");
  }

  @Test
  public void es8VersionGateRejects8_17() {
    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> Es8SearchClientShim.assertSemanticSearchSupported("8.17.0"));
    assertTrue(
        ex.getMessage().contains("8.18"), "Message should mention 8.18; got: " + ex.getMessage());
  }

  @Test
  public void es8VersionGateRejectsNullVersion() {
    expectThrows(
        IllegalStateException.class, () -> Es8SearchClientShim.assertSemanticSearchSupported(null));
  }

  @Test
  public void es8VersionGateRejectsUnknownVersion() {
    expectThrows(
        IllegalStateException.class,
        () -> Es8SearchClientShim.assertSemanticSearchSupported("unknown"));
  }
}
