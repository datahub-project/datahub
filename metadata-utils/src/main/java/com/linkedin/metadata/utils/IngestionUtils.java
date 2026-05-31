package com.linkedin.metadata.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

@Slf4j
public class IngestionUtils {

  private static final String PIPELINE_NAME = "pipeline_name";
  private static final String SOURCE_FIELD = "source";
  private static final String TYPE_FIELD = "type";

  private IngestionUtils() {}

  /**
   * Best-effort extraction of {@code source.type} from a recipe JSON document. Returns {@code null}
   * for any malformed input — callers fall back to a default rather than failing the request, since
   * a malformed recipe surfaces a clearer error downstream when the executor parses it.
   *
   * @param mapper the {@link ObjectMapper} to parse with — pass a shared instance (e.g. the
   *     OperationContext's) rather than allocating a new one per call
   * @param recipeJson the recipe JSON string, may be {@code null} or empty
   * @return the {@code source.type} value, or {@code null} if absent / not derivable
   */
  @Nullable
  public static String extractSourceType(
      @Nonnull final ObjectMapper mapper, @Nullable final String recipeJson) {
    if (recipeJson == null || recipeJson.isEmpty()) {
      return null;
    }
    try {
      // path() returns a missing node when a segment is absent, so the chained lookup handles
      // missing source / missing type uniformly without explicit has() checks.
      JsonNode type = mapper.readTree(recipeJson).path(SOURCE_FIELD).path(TYPE_FIELD);
      return type.isTextual() && !type.asText().isEmpty() ? type.asText() : null;
    } catch (JsonProcessingException e) {
      log.debug("Could not extract source.type from recipe for version-matrix lookup", e);
      return null;
    }
  }

  /**
   * Injects a pipeline_name into a recipe if there isn't a pipeline_name already there. The
   * pipeline_name will be the urn of the ingestion source.
   *
   * @param pipelineName the new pipeline name in the recipe.
   * @return a modified recipe JSON string
   */
  public static String injectPipelineName(
      @Nonnull String originalJson, @Nonnull final String pipelineName) {
    try {
      final JSONObject jsonRecipe = new JSONObject(originalJson);
      boolean hasPipelineName =
          jsonRecipe.has(PIPELINE_NAME)
              && jsonRecipe.get(PIPELINE_NAME) != null
              && !jsonRecipe.get(PIPELINE_NAME).equals("");

      if (!hasPipelineName) {
        jsonRecipe.put(PIPELINE_NAME, pipelineName);
        return jsonRecipe.toString();
      }
    } catch (JSONException e) {
      throw new IllegalArgumentException(
          "Failed to create execution request: Invalid recipe json provided.", e);
    }
    return originalJson;
  }
}
