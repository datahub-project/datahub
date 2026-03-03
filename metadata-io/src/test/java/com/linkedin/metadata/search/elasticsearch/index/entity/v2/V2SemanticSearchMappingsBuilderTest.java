package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class V2SemanticSearchMappingsBuilderTest {

  private V2SemanticSearchMappingsBuilder semanticSearchMappingsBuilder;
  private OperationContext operationContext;

  @BeforeMethod
  public void setUp() {
    EntityIndexConfiguration entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);

    V2MappingsBuilder v2MappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    // Create semantic search configuration for testing - enable all entities with multiple models
    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEnabledEntities(
        Set.of(
            "dataset",
            "chart",
            "dashboard",
            "dataJob",
            "dataFlow",
            "container",
            "tag",
            "glossaryTerm",
            "domain",
            "corpuser",
            "corpGroup"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig cohereModel =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    cohereModel.setVectorDimension(1024);
    cohereModel.setKnnEngine("faiss");
    cohereModel.setSpaceType("cosinesimil");
    cohereModel.setEfConstruction(128);
    cohereModel.setM(16);

    com.linkedin.metadata.config.search.ModelEmbeddingConfig openaiModel =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    openaiModel.setVectorDimension(1536);
    openaiModel.setKnnEngine("faiss");
    openaiModel.setSpaceType("cosinesimil");
    openaiModel.setEfConstruction(128);
    openaiModel.setM(16);

    semanticConfig.setModels(
        Map.of("cohere_embed_v3", cohereModel, "openai_text_embedding_3_small", openaiModel));

    // Use real IndexConventionImpl instead of mocking
    com.linkedin.metadata.config.search.EntityIndexConfiguration entityIndexConfig =
        new com.linkedin.metadata.config.search.EntityIndexConfiguration();
    IndexConvention indexConvention =
        com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl.noPrefix(
            "MD5", entityIndexConfig);

    semanticSearchMappingsBuilder =
        new V2SemanticSearchMappingsBuilder(v2MappingsBuilder, semanticConfig, indexConvention);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testGetIndexMappingsAddsEmbeddingField() throws JsonProcessingException {
    Collection<IndexMapping> result =
        semanticSearchMappingsBuilder.getIndexMappings(operationContext, Collections.emptyList());

    assertFalse(result.isEmpty(), "Should create semantic search indices");

    for (var indexMapping : result) {
      // Verify index name has _semantic suffix
      assertTrue(
          indexMapping.getIndexName().endsWith("_semantic"),
          "Index name should end with _semantic: " + indexMapping.getIndexName());

      // Verify embeddings field exists
      @SuppressWarnings("unchecked")
      Map<String, Object> properties =
          (Map<String, Object>) indexMapping.getMappings().get("properties");

      assertTrue(
          properties.containsKey("embeddings"),
          "Mappings should contain embeddings field for " + indexMapping.getIndexName());

      // Verify embeddings field structure - now contains model-keyed subfields
      @SuppressWarnings("unchecked")
      Map<String, Object> embeddingsField = (Map<String, Object>) properties.get("embeddings");
      assertNotNull(embeddingsField, "Should have embeddings field");

      @SuppressWarnings("unchecked")
      Map<String, Object> embeddingsProperties =
          (Map<String, Object>) embeddingsField.get("properties");
      assertNotNull(embeddingsProperties, "Embeddings should have properties");

      // Verify model keys exist
      assertTrue(
          embeddingsProperties.containsKey("cohere_embed_v3"), "Should have cohere_embed_v3 model");
      assertTrue(
          embeddingsProperties.containsKey("openai_text_embedding_3_small"),
          "Should have openai_text_embedding_3_small model");

      // Verify cohere_embed_v3 model structure
      @SuppressWarnings("unchecked")
      Map<String, Object> cohereModel =
          (Map<String, Object>) embeddingsProperties.get("cohere_embed_v3");
      @SuppressWarnings("unchecked")
      Map<String, Object> cohereModelProperties =
          (Map<String, Object>) cohereModel.get("properties");

      // Verify chunks field
      assertTrue(cohereModelProperties.containsKey("chunks"), "Should have chunks field");
      @SuppressWarnings("unchecked")
      Map<String, Object> chunksField = (Map<String, Object>) cohereModelProperties.get("chunks");
      assertEquals(chunksField.get("type"), "nested", "Chunks should be nested type");

      @SuppressWarnings("unchecked")
      Map<String, Object> chunksProperties = (Map<String, Object>) chunksField.get("properties");
      assertTrue(chunksProperties.containsKey("vector"), "Should have vector field");
      assertTrue(chunksProperties.containsKey("text"), "Should have text field");
      assertTrue(chunksProperties.containsKey("position"), "Should have position field");
      assertTrue(
          chunksProperties.containsKey("characterOffset"), "Should have characterOffset field");
      assertTrue(
          chunksProperties.containsKey("characterLength"), "Should have characterLength field");
      assertTrue(chunksProperties.containsKey("tokenCount"), "Should have tokenCount field");

      // Verify vector field configuration
      @SuppressWarnings("unchecked")
      Map<String, Object> vectorField = (Map<String, Object>) chunksProperties.get("vector");
      assertEquals(vectorField.get("type"), "knn_vector", "Vector should be knn_vector type");
      assertEquals(vectorField.get("dimension"), 1024, "Cohere model should have 1024 dimensions");

      // Verify character metadata fields
      @SuppressWarnings("unchecked")
      Map<String, Object> characterOffsetField =
          (Map<String, Object>) chunksProperties.get("characterOffset");
      assertEquals(
          characterOffsetField.get("type"), "integer", "characterOffset should be integer type");

      @SuppressWarnings("unchecked")
      Map<String, Object> characterLengthField =
          (Map<String, Object>) chunksProperties.get("characterLength");
      assertEquals(
          characterLengthField.get("type"), "integer", "characterLength should be integer type");

      @SuppressWarnings("unchecked")
      Map<String, Object> tokenCountField =
          (Map<String, Object>) chunksProperties.get("tokenCount");
      assertEquals(tokenCountField.get("type"), "integer", "tokenCount should be integer type");

      @SuppressWarnings("unchecked")
      Map<String, Object> method = (Map<String, Object>) vectorField.get("method");
      assertNotNull(method, "Should have method configuration");
      assertEquals(method.get("name"), "hnsw", "Should use HNSW algorithm");
      assertEquals(method.get("engine"), "faiss", "Should use FAISS engine");
      assertEquals(method.get("space_type"), "cosinesimil", "Should use cosine similarity");

      // Verify metadata fields
      assertTrue(cohereModelProperties.containsKey("totalChunks"), "Should have totalChunks field");

      // Verify OpenAI model has different dimension
      @SuppressWarnings("unchecked")
      Map<String, Object> openaiModel =
          (Map<String, Object>) embeddingsProperties.get("openai_text_embedding_3_small");
      @SuppressWarnings("unchecked")
      Map<String, Object> openaiModelProperties =
          (Map<String, Object>) openaiModel.get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> openaiChunksField =
          (Map<String, Object>) openaiModelProperties.get("chunks");
      @SuppressWarnings("unchecked")
      Map<String, Object> openaiChunksProperties =
          (Map<String, Object>) openaiChunksField.get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> openaiVectorField =
          (Map<String, Object>) openaiChunksProperties.get("vector");
      assertEquals(
          openaiVectorField.get("dimension"), 1536, "OpenAI model should have 1536 dimensions");
    }
  }

  @Test
  public void testEntityFiltering() {
    // Create config with only dataset enabled
    SemanticSearchConfiguration limitedConfig = new SemanticSearchConfiguration();
    limitedConfig.setEnabled(true);
    limitedConfig.setEnabledEntities(Set.of("dataset"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig testModel =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    testModel.setVectorDimension(1024);
    testModel.setKnnEngine("faiss");
    testModel.setSpaceType("cosinesimil");
    testModel.setEfConstruction(128);
    testModel.setM(16);

    limitedConfig.setModels(Map.of("test_model", testModel));

    EntityIndexConfiguration entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);
    V2MappingsBuilder v2MappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    // Use real IndexConventionImpl instead of mocking
    com.linkedin.metadata.config.search.EntityIndexConfiguration entityIndexConfig =
        new com.linkedin.metadata.config.search.EntityIndexConfiguration();
    IndexConvention indexConvention =
        com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl.noPrefix(
            "MD5", entityIndexConfig);

    V2SemanticSearchMappingsBuilder limitedBuilder =
        new V2SemanticSearchMappingsBuilder(v2MappingsBuilder, limitedConfig, indexConvention);

    Collection<IndexMapping> result =
        limitedBuilder.getIndexMappings(operationContext, Collections.emptyList());

    // Should only create index for dataset
    assertTrue(result.size() >= 1, "Should create at least one semantic index");

    // All created indices should be for dataset
    for (var indexMapping : result) {
      assertTrue(
          indexMapping.getIndexName().contains("dataset"),
          "Only dataset indices should be created: " + indexMapping.getIndexName());
    }
  }
}
