package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;

public class EmbeddingUtils {
  /**
   * Creates the embedding update structure for a document.
   *
   * @param objectMapper Jackson ObjectMapper for creating JSON nodes
   * @param embedding The embedding vector to store
   * @param text The source text that was embedded (stored for reference)
   * @return ObjectNode containing the embeddings field structure ready for document update
   */
  public static ObjectNode createEmbeddingUpdate(
      ObjectMapper objectMapper,
      float[] embedding,
      String text,
      String embeddingField,
      String modelVersion) {
    ObjectNode embeddingData = objectMapper.createObjectNode();
    ObjectNode embeddings = embeddingData.putObject("embeddings");
    ObjectNode modelData = embeddings.putObject(embeddingField);

    modelData.put("model_version", modelVersion);
    modelData.put("generated_at", Instant.now().toString());
    modelData.put("total_chunks", 1);

    ObjectNode chunk = modelData.putArray("chunks").addObject();
    chunk.put("position", 0);
    chunk.put("text", text);
    chunk.put("character_offset", 0);
    chunk.put("character_length", text.length());
    chunk.put("token_count", text.split("\\s+").length);

    // Convert float array to JSON array
    var vectorArray = chunk.putArray("vector");
    for (float value : embedding) {
      vectorArray.add(value);
    }

    return embeddingData;
  }
}
