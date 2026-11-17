package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.testng.annotations.Test;

/**
 * Covers the startup checks that build the hybrid reranker in {@link ElasticSearchServiceFactory}.
 */
public class ElasticSearchServiceFactoryHybridTest {

  private final EmbeddingProvider provider = mock(EmbeddingProvider.class);

  @Test
  public void testNoRerankerUnlessHybridReadIsOnWithV3() {
    assertNull(build(entityIndex(v3 -> v3.hybridReadEnabled(false)), provider));
    assertNull(build(entityIndex(v3 -> v3.enabled(false)), provider));
  }

  @Test
  public void testBuildsTheRerankerWhenEveryRequirementIsMet() {
    assertNotNull(build(entityIndex(v3 -> {}), provider));
  }

  @Test
  public void testRequiresV3KeywordAndSemanticReads() {
    expectThrows(
        IllegalStateException.class,
        () -> build(entityIndex(v3 -> v3.keywordReadEnabled(false)), provider));
    expectThrows(
        IllegalStateException.class,
        () -> build(entityIndex(v3 -> v3.semanticReadEnabled(false)), provider));
  }

  @Test
  public void testRequiresSemanticSearchAndARealProvider() {
    EntityIndexConfiguration semanticOff = entityIndex(v3 -> {});
    semanticOff.getSemanticSearch().setEnabled(false);
    expectThrows(IllegalStateException.class, () -> build(semanticOff, provider));
    expectThrows(
        IllegalStateException.class,
        () -> build(entityIndex(v3 -> {}), new NoOpEmbeddingProvider()));
    expectThrows(IllegalStateException.class, () -> build(entityIndex(v3 -> {}), null));
  }

  @Test
  public void testRequiresTheActiveModelMapping() {
    EntityIndexConfiguration otherModel = entityIndex(v3 -> {});
    otherModel.getSemanticSearch().setModels(Map.of("other_model", new ModelEmbeddingConfig()));
    expectThrows(IllegalStateException.class, () -> build(otherModel, provider));

    EntityIndexConfiguration l1 = entityIndex(v3 -> {});
    l1.getSemanticSearch().getModels().get("text_embedding_3_small").setSpaceType("l1");
    expectThrows(IllegalStateException.class, () -> build(l1, provider));
  }

  @Test
  public void testRequiresOpenSearch35OnTheSearchV3Cluster() throws IOException {
    SearchClientShim<?> openSearch34 = mock(SearchClientShim.class);
    when(openSearch34.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_3);
    when(openSearch34.getEngineVersion()).thenReturn("3.4.0");

    expectThrows(
        IllegalStateException.class,
        () ->
            ElasticSearchServiceFactory.hybridSearchResultReranker(
                entityIndex(v3 -> {}), () -> provider, () -> openSearch34));
  }

  private static HybridSearchResultReranker build(
      EntityIndexConfiguration entityIndex, EmbeddingProvider provider) {
    SearchClientShim<?> elasticsearch = mock(SearchClientShim.class);
    when(elasticsearch.getEngineType()).thenReturn(SearchEngineType.ELASTICSEARCH_8);
    return ElasticSearchServiceFactory.hybridSearchResultReranker(
        entityIndex, () -> provider, () -> elasticsearch);
  }

  /** V3 with keyword, semantic and hybrid reads on, then {@code v3} adjusts the V3 settings. */
  private static EntityIndexConfiguration entityIndex(
      Consumer<EntityIndexVersionConfiguration.EntityIndexVersionConfigurationBuilder> v3) {
    EntityIndexVersionConfiguration.EntityIndexVersionConfigurationBuilder v3Builder =
        EntityIndexVersionConfiguration.builder()
            .enabled(true)
            .keywordReadEnabled(true)
            .semanticReadEnabled(true)
            .hybridReadEnabled(true);
    v3.accept(v3Builder);
    EmbeddingProviderConfiguration providerConfiguration = new EmbeddingProviderConfiguration();
    providerConfiguration.setType("openai");
    providerConfiguration.getOpenai().setModel("text-embedding-3-small");
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(1536);
    return EntityIndexConfiguration.builder()
        .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
        .v3(v3Builder.build())
        .semanticSearch(
            new SemanticSearchConfiguration(
                true,
                Set.of("document"),
                new HashMap<>(Map.of("text_embedding_3_small", model)),
                providerConfiguration))
        .build();
  }
}
