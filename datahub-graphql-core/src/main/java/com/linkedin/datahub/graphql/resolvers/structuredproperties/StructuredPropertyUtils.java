package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import com.linkedin.structured.StructuredPropertySettings;
import java.util.UUID;
import javax.annotation.Nullable;

public class StructuredPropertyUtils {
  /*
   * We accept both ID and qualifiedName as inputs when creating a structured property. However,
   * these two fields should ALWAYS be the same. If they don't provide either, use a UUID for both.
   * If they provide both, ensure they are the same otherwise throw. Otherwise, use what is provided.
   */
  public static String getPropertyId(
      @Nullable final String inputId, @Nullable final String inputQualifiedName) {
    if (inputId != null && inputQualifiedName != null && !inputId.equals(inputQualifiedName)) {
      throw new IllegalArgumentException(
          "Qualified name and the ID of a structured property must match");
    }

    String id = UUID.randomUUID().toString();

    if (inputQualifiedName != null) {
      id = inputQualifiedName;
    } else if (inputId != null) {
      id = inputId;
    }

    return id;
  }

  /*
   * Ensure that a structured property settings aspect is valid by ensuring that if isHidden is true,
   * the other fields concerning display locations are false;
   */
  public static void validatePropertySettings(StructuredPropertySettings settings)
      throws Exception {
    if (settings.isIsHidden()) {
      if (settings.isShowInSearchFilters()
          || settings.isShowInAssetSummary()
          || settings.isShowAsAssetBadge()
          || settings.isShowInColumnsTable()) {
        throw new IllegalArgumentException(
            "Cannot have property isHidden = true while other display location settings are also true.");
      }
    }
  }
}
