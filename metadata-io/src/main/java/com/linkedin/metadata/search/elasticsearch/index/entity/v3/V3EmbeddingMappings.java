package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Makes the V3 {@code embeddings} mapping explicit. The V3 root is {@code dynamic: true}, so an
 * undeclared key under {@code embeddings} would be mapped from the first document that carries it,
 * including a model that is no longer configured. Only the configured model objects and the fields
 * both writers store are mapped. The shared builders also feed the V2 sidecar and are left as they
 * are.
 */
final class V3EmbeddingMappings {

  /**
   * Profile fields the service and connectors store that the shared builders do not declare. A
   * field left out here would stay in {@code _source} and stop being searchable.
   */
  private static final Map<String, Object> UNDECLARED_PROFILE_FIELDS =
      Map.of(
          "modelVersion", Map.of("type", "keyword"),
          "chunkingStrategy", Map.of("type", "keyword"),
          "totalChunks", Map.of("type", "integer"),
          "totalTokens", Map.of("type", "integer"),
          "vectorDimension", Map.of("type", "integer"));

  private V3EmbeddingMappings() {}

  /** A copy of {@code embeddings} with {@code dynamic: false} on each object level. */
  @Nonnull
  @SuppressWarnings("unchecked")
  static Map<String, Object> explicit(@Nonnull final Map<String, Object> embeddings) {
    if (!(embeddings.get("properties") instanceof Map)) {
      return embeddings;
    }
    final Map<String, Object> models = (Map<String, Object>) embeddings.get("properties");
    final Map<String, Object> explicitModels = new HashMap<>();
    for (Map.Entry<String, Object> model : models.entrySet()) {
      explicitModels.put(
          model.getKey(),
          model.getValue() instanceof Map
              ? explicitModel((Map<String, Object>) model.getValue())
              : model.getValue());
    }
    final Map<String, Object> result = new HashMap<>(embeddings);
    result.put("dynamic", false);
    result.put("properties", explicitModels);
    return result;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> explicitModel(@Nonnull final Map<String, Object> model) {
    final Map<String, Object> result = new HashMap<>(model);
    result.put("dynamic", false);
    final Map<String, Object> fields =
        model.get("properties") instanceof Map
            ? new HashMap<>((Map<String, Object>) model.get("properties"))
            : new HashMap<>();
    UNDECLARED_PROFILE_FIELDS.forEach(fields::putIfAbsent);
    if (fields.get("chunks") instanceof Map) {
      final Map<String, Object> chunks = new HashMap<>((Map<String, Object>) fields.get("chunks"));
      chunks.put("dynamic", false);
      fields.put("chunks", chunks);
    }
    result.put("properties", fields);
    return result;
  }
}
