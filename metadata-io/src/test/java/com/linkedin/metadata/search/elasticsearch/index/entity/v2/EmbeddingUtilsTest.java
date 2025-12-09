package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.annotations.Test;

public class EmbeddingUtilsTest {

  @Test
  public void testCreateEmbeddingUpdate() {
    ObjectMapper mapper = new ObjectMapper();
    float[] embedding = {0.1f, 0.2f, 0.3f};
    String text = "test document";

    ObjectNode result =
        EmbeddingUtils.createEmbeddingUpdate(
            mapper, embedding, text, "cohere_embed_v3", "cohere.embed-english-v3");

    // Verify basic structure exists
    JsonNode modelData = result.get("embeddings").get("cohere_embed_v3");
    assertNotNull(modelData);
    assertEquals(modelData.get("model_version").asText(), "cohere.embed-english-v3");
    assertEquals(modelData.get("total_chunks").asInt(), 1);

    // Verify chunk has text and complete vector
    JsonNode chunk = modelData.get("chunks").get(0);
    assertEquals(chunk.get("text").asText(), "test document");

    JsonNode vector = chunk.get("vector");
    assertEquals(vector.size(), 3);
    assertEquals(vector.get(0).floatValue(), 0.1f, 0.001f);
    assertEquals(vector.get(1).floatValue(), 0.2f, 0.001f);
    assertEquals(vector.get(2).floatValue(), 0.3f, 0.001f);
  }
}
