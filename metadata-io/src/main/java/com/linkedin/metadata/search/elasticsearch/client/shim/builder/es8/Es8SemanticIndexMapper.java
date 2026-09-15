package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

public final class Es8SemanticIndexMapper {

  private Es8SemanticIndexMapper() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull SemanticIndexSpec spec) {
    Map<String, Object> indexOptions = new LinkedHashMap<>();
    indexOptions.put("type", "hnsw");
    indexOptions.put("m", spec.hnswM());
    indexOptions.put("ef_construction", spec.hnswEfConstruction());

    Map<String, Object> vector = new LinkedHashMap<>();
    vector.put("type", "dense_vector");
    vector.put("dims", spec.vectorDimension());
    vector.put("index", true);
    vector.put("similarity", spec.similarity());
    vector.put("index_options", indexOptions);

    Map<String, Object> chunkProps = new LinkedHashMap<>();
    chunkProps.put("vector", vector);
    chunkProps.put("text", Map.of("type", "text", "index", false));
    chunkProps.put("position", Map.of("type", "integer"));
    chunkProps.put("characterOffset", Map.of("type", "integer"));
    chunkProps.put("characterLength", Map.of("type", "integer"));
    chunkProps.put("tokenCount", Map.of("type", "integer"));

    Map<String, Object> chunks = new LinkedHashMap<>();
    chunks.put("type", "nested");
    chunks.put("properties", chunkProps);

    Map<String, Object> modelEntry = new LinkedHashMap<>();
    modelEntry.put("properties", Map.of("chunks", chunks));

    Map<String, Object> embeddingsProps = new LinkedHashMap<>();
    embeddingsProps.put(spec.modelKey(), modelEntry);

    Map<String, Object> embeddings = new LinkedHashMap<>();
    embeddings.put("properties", embeddingsProps);

    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("urn", Map.of("type", "keyword"));
    properties.put("embeddings", embeddings);

    Map<String, Object> mapping = new LinkedHashMap<>();
    mapping.put("properties", properties);
    return mapping;
  }
}
