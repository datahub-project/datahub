package com.linkedin.metadata.config.usage.cigate;

import java.io.File;
import java.nio.file.Path;
import javax.annotation.Nonnull;

/** Resolves DataHub repo root for usage registry CI gates. */
public final class UsageRegistryRepoPaths {

  private UsageRegistryRepoPaths() {}

  @Nonnull
  public static Path repoRoot() {
    String override = System.getProperty("datahub.repoRoot");
    if (override != null && !override.isBlank()) {
      return Path.of(override).toAbsolutePath().normalize();
    }
    Path cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    Path current = cwd;
    while (current != null) {
      if (isRepoRoot(current)) {
        return current;
      }
      current = current.getParent();
    }
    throw new IllegalStateException(
        "Could not locate DataHub repo root from user.dir="
            + cwd
            + "; set -Ddatahub.repoRoot=/path/to/datahub");
  }

  private static boolean isRepoRoot(@Nonnull Path path) {
    return new File(path.toFile(), "datahub-graphql-core").isDirectory()
        && new File(path.toFile(), "metadata-service/configuration").isDirectory();
  }
}
