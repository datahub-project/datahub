package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import static org.testng.Assert.*;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.Map;
import org.testng.annotations.Test;

public class OpenSearch2SemanticIndexMapperTest {

  private SemanticIndexSpec spec() {
    return SemanticIndexSpec.builder()
        .indexName("datasetindex_v2_semantic")
        .modelKey("text_embedding_3_large")
        .vectorDimension(3072)
        .similarity("cosinesimil")
        .hnswM(16)
        .hnswEfConstruction(128)
        .build();
  }

  @Test
  public void testKnnVectorFieldType() {
    Map<String, Object> mapping = OpenSearch2SemanticIndexMapper.build(spec());

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
    assertNotNull(properties, "Mapping should have top-level properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> embeddings = (Map<String, Object>) properties.get("embeddings");
    assertNotNull(embeddings, "Should have embeddings field");

    @SuppressWarnings("unchecked")
    Map<String, Object> embeddingsProps = (Map<String, Object>) embeddings.get("properties");
    assertNotNull(embeddingsProps, "embeddings should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> modelEntry =
        (Map<String, Object>) embeddingsProps.get("text_embedding_3_large");
    assertNotNull(modelEntry, "Should have model key entry");

    @SuppressWarnings("unchecked")
    Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
    assertNotNull(modelProps, "Model entry should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> chunks = (Map<String, Object>) modelProps.get("chunks");
    assertNotNull(chunks, "Should have chunks field");
    assertEquals(chunks.get("type"), "nested", "chunks should be nested type");

    @SuppressWarnings("unchecked")
    Map<String, Object> chunkProps = (Map<String, Object>) chunks.get("properties");
    assertNotNull(chunkProps, "chunks should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> vector = (Map<String, Object>) chunkProps.get("vector");
    assertNotNull(vector, "chunks should have vector field");
    assertEquals(vector.get("type"), "knn_vector", "vector type should be knn_vector for OS");
    assertEquals(vector.get("dimension"), 3072, "dimension should match spec vectorDimension");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHnswMethodConfig() {
    Map<String, Object> mapping = OpenSearch2SemanticIndexMapper.build(spec());

    Map<String, Object> topProps = (Map<String, Object>) mapping.get("properties");
    Map<String, Object> embeddings = (Map<String, Object>) topProps.get("embeddings");
    Map<String, Object> embeddingsProps = (Map<String, Object>) embeddings.get("properties");
    Map<String, Object> modelEntry =
        (Map<String, Object>) embeddingsProps.get("text_embedding_3_large");
    Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
    Map<String, Object> chunks = (Map<String, Object>) modelProps.get("chunks");
    Map<String, Object> chunkProps = (Map<String, Object>) chunks.get("properties");
    Map<String, Object> vector = (Map<String, Object>) chunkProps.get("vector");

    Map<String, Object> method = (Map<String, Object>) vector.get("method");
    assertNotNull(method, "vector should have method block");
    assertEquals(method.get("name"), "hnsw", "method name should be hnsw");
    assertEquals(method.get("engine"), "faiss", "engine should be faiss");
    assertEquals(method.get("space_type"), "cosinesimil", "space_type should match spec");

    Map<String, Object> params = (Map<String, Object>) method.get("parameters");
    assertNotNull(params, "method should have parameters");
    assertEquals(params.get("m"), 16, "m should match spec hnswM");
    assertEquals(params.get("ef_construction"), 128, "ef_construction should match spec");
  }

  @Test
  public void testNoDenseVectorType() {
    Map<String, Object> mapping = OpenSearch2SemanticIndexMapper.build(spec());
    assertFalse(
        mapping.toString().contains("dense_vector"),
        "OS mapping must not contain ES8-specific dense_vector field type");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testModelLevelMetadataFields() {
    Map<String, Object> mapping = OpenSearch2SemanticIndexMapper.build(spec());

    Map<String, Object> topProps = (Map<String, Object>) mapping.get("properties");
    Map<String, Object> embeddings = (Map<String, Object>) topProps.get("embeddings");
    Map<String, Object> embeddingsProps = (Map<String, Object>) embeddings.get("properties");
    Map<String, Object> modelEntry =
        (Map<String, Object>) embeddingsProps.get("text_embedding_3_large");
    Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");

    assertEquals(
        modelProps.keySet().size(), 1, "Model entry should only contain chunks (no unused fields)");
    assertTrue(modelProps.containsKey("chunks"), "Should have chunks field");
  }
}
