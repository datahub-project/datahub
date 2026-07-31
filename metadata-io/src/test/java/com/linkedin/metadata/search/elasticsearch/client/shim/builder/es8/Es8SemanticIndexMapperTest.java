package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import static org.testng.Assert.*;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.Map;
import org.testng.annotations.Test;

public class Es8SemanticIndexMapperTest {

  private SemanticIndexSpec spec() {
    return SemanticIndexSpec.builder()
        .indexName("datasetindex_v2_semantic")
        .modelKey("gemini_embedding_001")
        .vectorDimension(3072)
        .similarity("cosine")
        .hnswM(16)
        .hnswEfConstruction(128)
        .build();
  }

  @Test
  public void testDenseVectorFieldType() {
    Map<String, Object> mapping = Es8SemanticIndexMapper.build(spec());

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
        (Map<String, Object>) embeddingsProps.get("gemini_embedding_001");
    assertNotNull(modelEntry, "Should have model key gemini_embedding_001");

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
    assertEquals(vector.get("type"), "dense_vector", "vector type should be dense_vector");
    assertEquals(vector.get("dims"), 3072, "dims should match spec vectorDimension");
    assertEquals(vector.get("index"), true, "index should be true");
    assertEquals(vector.get("similarity"), "cosine", "similarity should match spec");

    @SuppressWarnings("unchecked")
    Map<String, Object> indexOptions = (Map<String, Object>) vector.get("index_options");
    assertNotNull(indexOptions, "vector should have index_options");
    assertEquals(indexOptions.get("type"), "hnsw", "index_options type should be hnsw");
    assertEquals(indexOptions.get("m"), 16, "m should match spec hnswM");
    assertEquals(indexOptions.get("ef_construction"), 128, "ef_construction should match spec");
  }

  @Test
  public void testChunkTextFieldNotIndexed() {
    Map<String, Object> mapping = Es8SemanticIndexMapper.build(spec());

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> embeddings = (Map<String, Object>) properties.get("embeddings");
    @SuppressWarnings("unchecked")
    Map<String, Object> embeddingsProps = (Map<String, Object>) embeddings.get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> modelEntry =
        (Map<String, Object>) embeddingsProps.get("gemini_embedding_001");
    @SuppressWarnings("unchecked")
    Map<String, Object> modelProps = (Map<String, Object>) modelEntry.get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> chunks = (Map<String, Object>) modelProps.get("chunks");
    @SuppressWarnings("unchecked")
    Map<String, Object> chunkProps = (Map<String, Object>) chunks.get("properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> text = (Map<String, Object>) chunkProps.get("text");
    assertNotNull(text, "text field must exist in chunk properties");
    assertEquals(text.get("type"), "text", "text field type should be 'text'");
    assertEquals(
        text.get("index"),
        false,
        "chunk text field must have index:false — it is never queried by keyword, only stored");
  }

  @Test
  public void testNoKnnVectorType() {
    Map<String, Object> mapping = Es8SemanticIndexMapper.build(spec());
    assertFalse(
        mapping.toString().contains("knn_vector"),
        "ES 8 mapping must not contain OpenSearch-specific knn_vector field type");
  }
}
