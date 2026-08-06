package com.linkedin.metadata.config.usage.cigate;

import javax.annotation.Nonnull;

/** Matches git changed paths against usage registry CI gate watch lists. */
public final class UsageRegistryGateChangeMatcher {

  private UsageRegistryGateChangeMatcher() {}

  public static boolean isGraphqlGatePath(
      @Nonnull String path, @Nonnull UsageRegistryCiGateProfile profile) {
    if (profile.graphqlWatchedExactPaths().contains(path)) {
      return true;
    }
    return profile.graphqlWatchedPrefixSuffixes().stream().anyMatch(path::startsWith)
        && path.endsWith(".graphql");
  }

  public static boolean isOpenApiGatePath(
      @Nonnull String path, @Nonnull UsageRegistryCiGateProfile profile) {
    if (profile.openApiWatchedExactPaths().contains(path)) {
      return true;
    }
    return profile.openApiWatchedPrefixSuffixes().stream().anyMatch(path::startsWith);
  }

  public static boolean isRestLiGatePath(
      @Nonnull String path, @Nonnull UsageRegistryCiGateProfile profile) {
    if (profile.restLiWatchedExactPaths().contains(path)) {
      return true;
    }
    return profile.restLiWatchedPrefixSuffixes().stream().anyMatch(path::startsWith);
  }
}
