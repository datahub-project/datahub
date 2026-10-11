package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class DocumentV3EmbeddingMappingContributorTest {

  @Test
  public void testDocumentIndexGetsEmbeddingsOnOpenSearch() {
    DocumentV3EmbeddingMappingContributor contributor =
        new DocumentV3EmbeddingMappingContributor(semanticConfig("document"), osShim());
    Map<String, Object> extras = contributor.extraRootProperties("document");
    assertTrue(extras.containsKey("embeddings"));
    assertTrue(extras.containsKey("resolvedTextSha256"));
    assertEquals(vectorType(extras), "knn_vector");
  }

  @Test
  public void testDocumentIndexGetsDenseVectorOnElasticsearch() {
    for (SearchEngineType engine :
        new SearchEngineType[] {
          SearchEngineType.ELASTICSEARCH_8, SearchEngineType.ELASTICSEARCH_9
        }) {
      SearchClientShim<?> shim = mock(SearchClientShim.class);
      when(shim.getEngineType()).thenReturn(engine);
      DocumentV3EmbeddingMappingContributor contributor =
          new DocumentV3EmbeddingMappingContributor(semanticConfig("document"), shim);
      Map<String, Object> extras = contributor.extraRootProperties("document");
      assertEquals(vectorType(extras), "dense_vector", "engine=" + engine);
      assertFalse(
          vectorType(extras).equals("knn_vector"),
          "ES mappings must not use knn_vector: " + engine);
    }
  }

  @Test
  public void testOpenSearch3UsesKnnVector() {
    SearchClientShim<?> shim = mock(SearchClientShim.class);
    when(shim.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_3);
    DocumentV3EmbeddingMappingContributor contributor =
        new DocumentV3EmbeddingMappingContributor(semanticConfig("document"), shim);
    assertEquals(vectorType(contributor.extraRootProperties("document")), "knn_vector");
  }

  @Test
  public void testDatasetIndexGetsNoEmbeddingsByDefault() {
    DocumentV3EmbeddingMappingContributor contributor =
        new DocumentV3EmbeddingMappingContributor(semanticConfig("document"), osShim());
    assertTrue(contributor.extraRootProperties("dataset").isEmpty());
  }

  @Test
  public void testSemanticDisabledYieldsNoEmbeddings() {
    SemanticSearchConfiguration config = semanticConfig("document");
    config.setEnabled(false);
    DocumentV3EmbeddingMappingContributor contributor =
        new DocumentV3EmbeddingMappingContributor(config, osShim());
    assertTrue(contributor.extraRootProperties("document").isEmpty());
  }

  @Test
  public void testEnabledEntitiesDatasetGetsEmbeddings() {
    DocumentV3EmbeddingMappingContributor contributor =
        new DocumentV3EmbeddingMappingContributor(semanticConfig("dataset"), osShim());
    assertTrue(contributor.extraRootProperties("dataset").containsKey("embeddings"));
    assertTrue(contributor.extraRootProperties("document").isEmpty());
  }

  private static SemanticSearchConfiguration semanticConfig(String entity) {
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setEnabled(true);
    config.setEnabledEntities(Set.of(entity));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(1024);
    model.setKnnEngine("faiss");
    model.setSpaceType("cosinesimil");
    model.setEfConstruction(128);
    model.setM(16);
    config.setModels(Map.of("cohere_embed_v3", model));
    return config;
  }

  private static SearchClientShim<?> osShim() {
    SearchClientShim<?> shim = mock(SearchClientShim.class);
    when(shim.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);
    return shim;
  }

  @SuppressWarnings("unchecked")
  private static String vectorType(Map<String, Object> extras) {
    Map<String, Object> embeddings = (Map<String, Object>) extras.get("embeddings");
    Map<String, Object> properties = (Map<String, Object>) embeddings.get("properties");
    Map<String, Object> model = (Map<String, Object>) properties.get("cohere_embed_v3");
    Map<String, Object> modelProps = (Map<String, Object>) model.get("properties");
    Map<String, Object> chunks = (Map<String, Object>) modelProps.get("chunks");
    Map<String, Object> chunkProps = (Map<String, Object>) chunks.get("properties");
    Map<String, Object> vector = (Map<String, Object>) chunkProps.get("vector");
    return (String) vector.get("type");
  }
}
