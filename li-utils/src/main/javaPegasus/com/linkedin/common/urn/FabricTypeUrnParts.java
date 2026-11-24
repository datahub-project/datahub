package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.common.FabricTypeNames;

/**
 * The env part of URNs whose key holds a FabricTypeName: a FabricType symbol or the id of a
 * registered customFabricType. URN classes store it as a string so custom values survive parsing.
 */
final class FabricTypeUrnParts {

  private FabricTypeUrnParts() {}

  /** Rejects a value that is neither a FabricType symbol nor shaped like one. */
  static String checkEnv(String env) {
    // Built-in envs are the common case, so they pass without the regex.
    if (FabricTypeNames.isBuiltIn(env)) {
      return env;
    }
    if (!FabricTypeNames.isWellFormed(env)) {
      throw new IllegalArgumentException(
          "Invalid environment \""
              + env
              + "\": use a FabricType value or the id of a registered customFabricType (uppercase"
              + " letters, digits and underscores, starting with a letter)");
    }
    return env;
  }

  /** The FabricType for a built-in env, or {@code $UNKNOWN} for a custom one. */
  static FabricType origin(String env) {
    return FabricTypeNames.isBuiltIn(env) ? FabricType.valueOf(env) : FabricType.$UNKNOWN;
  }
}
