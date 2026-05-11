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
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
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

  private static SemanticSearchConfiguration buildTestSemanticConfig() {
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
    return semanticConfig;
  }

  @BeforeMethod
  public void setUp() {
    EntityIndexConfiguration entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);

    V2MappingsBuilder v2MappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    SemanticSearchConfiguration semanticConfig = buildTestSemanticConfig();

    // Use real IndexConventionImpl instead of mocking
    com.linkedin.metadata.config.search.EntityIndexConfiguration entityIndexConfig =
        new com.linkedin.metadata.config.search.EntityIndexConfiguration();
    IndexConvention indexConvention =
        com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl.noPrefix(
            "MD5", entityIndexConfig);

    // Default: OS 2 engine (knn_vector)
    SearchClientShim<?> osShim = mock(SearchClientShim.class);
    when(osShim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    semanticSearchMappingsBuilder =
        new V2SemanticSearchMappingsBuilder(
            v2MappingsBuilder, semanticConfig, indexConvention, osShim);
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

      // Verify model entry only has chunks (metadata fields like totalChunks removed — not
      // populated)
      assertEquals(cohereModelProperties.keySet().size(), 1, "Model should only contain chunks");
      assertTrue(cohereModelProperties.containsKey("chunks"), "Should have chunks field");

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

    SearchClientShim<?> osShim = mock(SearchClientShim.class);
    when(osShim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    V2SemanticSearchMappingsBuilder limitedBuilder =
        new V2SemanticSearchMappingsBuilder(
            v2MappingsBuilder, limitedConfig, indexConvention, osShim);

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

  @Test
  public void testEs8EngineUsesDenseVector() throws JsonProcessingException {
    EntityIndexConfiguration entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);
    V2MappingsBuilder v2MappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    SemanticSearchConfiguration semanticConfig = buildTestSemanticConfig();

    com.linkedin.metadata.config.search.EntityIndexConfiguration entityIndexConfig =
        new com.linkedin.metadata.config.search.EntityIndexConfiguration();
    IndexConvention indexConvention =
        com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl.noPrefix(
            "MD5", entityIndexConfig);

    SearchClientShim<?> es8Shim = mock(SearchClientShim.class);
    when(es8Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);

    V2SemanticSearchMappingsBuilder es8Builder =
        new V2SemanticSearchMappingsBuilder(
            v2MappingsBuilder, semanticConfig, indexConvention, es8Shim);

    Collection<IndexMapping> result =
        es8Builder.getIndexMappings(operationContext, Collections.emptyList());

    assertFalse(result.isEmpty(), "Should produce semantic index mappings for ES 8");

    for (IndexMapping indexMapping : result) {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties =
          (Map<String, Object>) indexMapping.getMappings().get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> embeddingsProps =
          (Map<String, Object>)
              ((Map<String, Object>) properties.get("embeddings")).get("properties");

      for (String modelKey : embeddingsProps.keySet()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> modelEntry = (Map<String, Object>) embeddingsProps.get(modelKey);
        @SuppressWarnings("unchecked")
        Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> chunksProps =
            (Map<String, Object>)
                ((Map<String, Object>) modelProps.get("chunks")).get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> vectorField = (Map<String, Object>) chunksProps.get("vector");

        assertEquals(
            vectorField.get("type"),
            "dense_vector",
            "ES 8 engine should use dense_vector, not knn_vector for model " + modelKey);

        // ES8 uses "dims" (not "dimension") — verify the key name
        assertNotNull(
            vectorField.get("dims"), "ES8 dense_vector should use 'dims', not 'dimension'");
        assertNull(
            vectorField.get("dimension"),
            "ES8 dense_vector must not use 'dimension' (that is the OS2 key)");

        // Similarity should be translated from OpenSearch space type
        assertNotNull(vectorField.get("similarity"), "ES8 dense_vector must have similarity field");
        String similarity = (String) vectorField.get("similarity");
        // cosinesimil -> cosine
        assertEquals(
            similarity,
            "cosine",
            "cosinesimil should be translated to 'cosine' for ES8 for model " + modelKey);

        // index should be true
        assertEquals(vectorField.get("index"), true, "ES8 dense_vector index must be true");

        // Verify index_options structure
        @SuppressWarnings("unchecked")
        Map<String, Object> indexOptions = (Map<String, Object>) vectorField.get("index_options");
        assertNotNull(indexOptions, "ES8 dense_vector must have index_options");
        assertEquals(
            indexOptions.get("type"), "hnsw", "index_options.type must be hnsw for " + modelKey);
        assertNotNull(
            indexOptions.get("m"), "index_options.m must be present for model " + modelKey);
        assertNotNull(
            indexOptions.get("ef_construction"),
            "index_options.ef_construction must be present for model " + modelKey);
      }
    }
  }

  /**
   * Verify that translateSpaceType correctly maps OpenSearch space types to ES8 similarity names.
   */
  @Test
  public void testTranslateSpaceTypeCosinesimil() throws JsonProcessingException {
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setEnabled(true);
    config.setEnabledEntities(Set.of("dataset"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig model =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    model.setVectorDimension(128);
    model.setKnnEngine("faiss");
    model.setSpaceType("cosinesimil");
    model.setEfConstruction(64);
    model.setM(8);
    config.setModels(Map.of("test_model", model));

    assertSimilarityTranslation(config, "cosine");
  }

  @Test
  public void testTranslateSpaceTypeL2() throws JsonProcessingException {
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setEnabled(true);
    config.setEnabledEntities(Set.of("dataset"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig model =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    model.setVectorDimension(128);
    model.setKnnEngine("faiss");
    model.setSpaceType("l2");
    model.setEfConstruction(64);
    model.setM(8);
    config.setModels(Map.of("test_model", model));

    assertSimilarityTranslation(config, "l2_norm");
  }

  @Test
  public void testTranslateSpaceTypeInnerProduct() throws JsonProcessingException {
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setEnabled(true);
    config.setEnabledEntities(Set.of("dataset"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig model =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    model.setVectorDimension(128);
    model.setKnnEngine("faiss");
    model.setSpaceType("innerproduct");
    model.setEfConstruction(64);
    model.setM(8);
    config.setModels(Map.of("test_model", model));

    assertSimilarityTranslation(config, "dot_product");
  }

  @Test
  public void testTranslateSpaceTypePassthroughForEs8Vocabulary() throws JsonProcessingException {
    // "cosine" is already in ES8 vocabulary — should pass through unchanged
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setEnabled(true);
    config.setEnabledEntities(Set.of("dataset"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig model =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    model.setVectorDimension(128);
    model.setKnnEngine("faiss");
    model.setSpaceType("cosine");
    model.setEfConstruction(64);
    model.setM(8);
    config.setModels(Map.of("test_model", model));

    assertSimilarityTranslation(config, "cosine");
  }

  @SuppressWarnings("unchecked")
  private void assertSimilarityTranslation(
      SemanticSearchConfiguration config, String expectedSimilarity)
      throws JsonProcessingException {
    EntityIndexConfiguration entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);
    V2MappingsBuilder v2MappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    com.linkedin.metadata.config.search.EntityIndexConfiguration entityIndexConfig =
        new com.linkedin.metadata.config.search.EntityIndexConfiguration();
    IndexConvention indexConvention =
        com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl.noPrefix(
            "MD5", entityIndexConfig);

    SearchClientShim<?> es8Shim = mock(SearchClientShim.class);
    when(es8Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);

    V2SemanticSearchMappingsBuilder builder =
        new V2SemanticSearchMappingsBuilder(v2MappingsBuilder, config, indexConvention, es8Shim);

    Collection<IndexMapping> result =
        builder.getIndexMappings(operationContext, Collections.emptyList());

    assertFalse(result.isEmpty(), "Should produce at least one mapping");

    for (IndexMapping indexMapping : result) {
      Map<String, Object> properties =
          (Map<String, Object>) indexMapping.getMappings().get("properties");
      Map<String, Object> embeddingsProps =
          (Map<String, Object>)
              ((Map<String, Object>) properties.get("embeddings")).get("properties");

      for (String modelKey : embeddingsProps.keySet()) {
        Map<String, Object> modelEntry = (Map<String, Object>) embeddingsProps.get(modelKey);
        Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
        Map<String, Object> chunksProps =
            (Map<String, Object>)
                ((Map<String, Object>) modelProps.get("chunks")).get("properties");
        Map<String, Object> vectorField = (Map<String, Object>) chunksProps.get("vector");

        assertEquals(
            vectorField.get("similarity"),
            expectedSimilarity,
            "similarity should be '" + expectedSimilarity + "' for model " + modelKey);
      }
    }
  }

  @Test
  public void testReverseTranslateEs8VocabToOpenSearch() throws JsonProcessingException {
    // "cosine" is ES8 vocabulary — should become "cosinesimil" on OpenSearch 2
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setEnabled(true);
    config.setEnabledEntities(Set.of("dataset"));

    com.linkedin.metadata.config.search.ModelEmbeddingConfig model =
        new com.linkedin.metadata.config.search.ModelEmbeddingConfig();
    model.setVectorDimension(768);
    model.setKnnEngine("lucene");
    model.setSpaceType("cosine");
    model.setEfConstruction(128);
    model.setM(16);
    config.setModels(Map.of("gemini_embedding_001", model));

    assertSpaceTypeForOpenSearch(config, "cosinesimil");
  }

  @SuppressWarnings("unchecked")
  private void assertSpaceTypeForOpenSearch(
      SemanticSearchConfiguration config, String expectedSpaceType) throws JsonProcessingException {
    EntityIndexConfiguration entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);
    V2MappingsBuilder v2MappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    com.linkedin.metadata.config.search.EntityIndexConfiguration entityIndexConfig =
        new com.linkedin.metadata.config.search.EntityIndexConfiguration();
    IndexConvention indexConvention =
        com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl.noPrefix(
            "MD5", entityIndexConfig);

    SearchClientShim<?> os2Shim = mock(SearchClientShim.class);
    when(os2Shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    V2SemanticSearchMappingsBuilder builder =
        new V2SemanticSearchMappingsBuilder(v2MappingsBuilder, config, indexConvention, os2Shim);

    Collection<IndexMapping> result =
        builder.getIndexMappings(operationContext, Collections.emptyList());

    assertFalse(result.isEmpty(), "Should produce at least one mapping");

    for (IndexMapping indexMapping : result) {
      Map<String, Object> properties =
          (Map<String, Object>) indexMapping.getMappings().get("properties");
      Map<String, Object> embeddingsProps =
          (Map<String, Object>)
              ((Map<String, Object>) properties.get("embeddings")).get("properties");

      for (String modelKey : embeddingsProps.keySet()) {
        Map<String, Object> modelEntry = (Map<String, Object>) embeddingsProps.get(modelKey);
        Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
        Map<String, Object> chunksProps =
            (Map<String, Object>)
                ((Map<String, Object>) modelProps.get("chunks")).get("properties");
        Map<String, Object> vectorField = (Map<String, Object>) chunksProps.get("vector");
        Map<String, Object> method = (Map<String, Object>) vectorField.get("method");

        assertEquals(
            method.get("space_type"),
            expectedSpaceType,
            "space_type should be reverse-translated to '"
                + expectedSpaceType
                + "' for model "
                + modelKey);
      }
    }
  }

  @Test
  public void testOpenSearchEngineUsesKnnVector() throws JsonProcessingException {
    Collection<IndexMapping> result =
        semanticSearchMappingsBuilder.getIndexMappings(operationContext, Collections.emptyList());

    assertFalse(result.isEmpty(), "Should produce semantic index mappings for OS");

    for (IndexMapping indexMapping : result) {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties =
          (Map<String, Object>) indexMapping.getMappings().get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> embeddingsProps =
          (Map<String, Object>)
              ((Map<String, Object>) properties.get("embeddings")).get("properties");

      for (String modelKey : embeddingsProps.keySet()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> modelEntry = (Map<String, Object>) embeddingsProps.get(modelKey);
        @SuppressWarnings("unchecked")
        Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> chunksProps =
            (Map<String, Object>)
                ((Map<String, Object>) modelProps.get("chunks")).get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> vectorField = (Map<String, Object>) chunksProps.get("vector");

        assertEquals(
            vectorField.get("type"),
            "knn_vector",
            "OS engine should use knn_vector for model " + modelKey);
      }
    }
  }
}
