package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Builds the OpenSearch 2.x index mapping for a semantic search index from a {@link
 * SemanticIndexSpec}.
 *
 * <p>OpenSearch uses the {@code knn_vector} field type with an HNSW method block. This differs from
 * Elasticsearch 8, which uses {@code dense_vector} with {@code index_options}.
 *
 * <p>The mapping structure produced is:
 *
 * <pre>{@code
 * {
 *   "properties": {
 *     "urn": { "type": "keyword" },
 *     "embeddings": {
 *       "properties": {
 *         "<modelKey>": {
 *           "properties": {
 *             "chunks": {
 *               "type": "nested",
 *               "properties": {
 *                 "vector": {
 *                   "type": "knn_vector",
 *                   "dimension": <vectorDimension>,
 *                   "method": {
 *                     "name": "hnsw",
 *                     "space_type": "<similarity>",
 *                     "engine": "faiss",
 *                     "parameters": { "ef_construction": <hnswEfConstruction>, "m": <hnswM> }
 *                   }
 *                 },
 *                 "text": { "type": "text", "index": false },
 *                 "position": { "type": "integer" },
 *                 "characterOffset": { "type": "integer" },
 *                 "characterLength": { "type": "integer" },
 *                 "tokenCount": { "type": "integer" }
 *               }
 *             },
 *             "totalChunks": { "type": "integer" },
 *             "modelVersion": { "type": "keyword" },
 *             "generatedAt": { "type": "date" }
 *           }
 *         }
 *       }
 *     }
 *   }
 * }
 * }</pre>
 */
public final class OpenSearch2SemanticIndexMapper {

  private OpenSearch2SemanticIndexMapper() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull SemanticIndexSpec spec) {
    Map<String, Object> methodParams = new HashMap<>();
    methodParams.put("ef_construction", spec.hnswEfConstruction());
    methodParams.put("m", spec.hnswM());

    Map<String, Object> method = new HashMap<>();
    method.put("name", "hnsw");
    method.put("space_type", spec.similarity());
    method.put("engine", spec.knnEngine());
    method.put("parameters", methodParams);

    Map<String, Object> vectorField = new HashMap<>();
    vectorField.put("type", "knn_vector");
    vectorField.put("dimension", spec.vectorDimension());
    vectorField.put("method", method);

    Map<String, Object> textField = new HashMap<>();
    textField.put("type", "text");
    textField.put("index", false);

    Map<String, Object> chunksProperties = new LinkedHashMap<>();
    chunksProperties.put("vector", vectorField);
    chunksProperties.put("text", textField);
    chunksProperties.put("position", ImmutableMap.of("type", "integer"));
    chunksProperties.put("characterOffset", ImmutableMap.of("type", "integer"));
    chunksProperties.put("characterLength", ImmutableMap.of("type", "integer"));
    chunksProperties.put("tokenCount", ImmutableMap.of("type", "integer"));

    Map<String, Object> chunksField = new LinkedHashMap<>();
    chunksField.put("type", "nested");
    chunksField.put("properties", chunksProperties);

    Map<String, Object> modelLevelProperties = new LinkedHashMap<>();
    modelLevelProperties.put("chunks", chunksField);

    Map<String, Object> modelFieldConfig = ImmutableMap.of("properties", modelLevelProperties);

    Map<String, Object> embeddingsProps = new LinkedHashMap<>();
    embeddingsProps.put(spec.modelKey(), modelFieldConfig);

    Map<String, Object> embeddings = ImmutableMap.of("properties", embeddingsProps);

    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("urn", ImmutableMap.of("type", "keyword"));
    properties.put("embeddings", embeddings);

    Map<String, Object> mapping = new LinkedHashMap<>();
    mapping.put("properties", properties);
    return mapping;
  }
}
