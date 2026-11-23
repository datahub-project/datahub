package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.FabricTypeNames;
import com.linkedin.datahub.graphql.generated.FabricType;
import javax.annotation.Nullable;

/**
 * Maps a stored environment name to the GraphQL {@link FabricType}. A custom environment has no
 * GraphQL symbol, so it maps to {@link FabricType#OTHER}; the actual value is in the env field.
 */
public final class FabricTypeMapper {

  private FabricTypeMapper() {}

  @Nullable
  public static FabricType map(@Nullable String env) {
    if (env == null) {
      return null;
    }
    return FabricTypeNames.isBuiltIn(env) ? FabricType.valueOf(env) : FabricType.OTHER;
  }

  /**
   * The urn of the customFabricType behind a custom env; null for a built-in, absent or malformed
   * env.
   */
  @Nullable
  public static String customFabricTypeUrn(@Nullable String env) {
    if (env == null || FabricTypeNames.isBuiltIn(env) || !FabricTypeNames.isWellFormed(env)) {
      return null;
    }
    return "urn:li:" + com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME + ":" + env;
  }
}
