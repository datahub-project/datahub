package com.linkedin.metadata.utils;

import javax.annotation.Nonnull;
import org.json.JSONException;
import org.json.JSONObject;

public class IngestionUtils {

  private static final String PIPELINE_NAME = "pipeline_name";

  private IngestionUtils() {}

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
