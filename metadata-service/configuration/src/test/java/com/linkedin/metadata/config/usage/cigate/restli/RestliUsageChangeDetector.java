package com.linkedin.metadata.config.usage.cigate.restli;

import com.linkedin.metadata.config.usage.cigate.GitChangeDetector;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfile;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfiles;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryGateChangeMatcher;
import java.nio.file.Path;
import javax.annotation.Nonnull;

public final class RestliUsageChangeDetector {

  private RestliUsageChangeDetector() {}

  public static boolean hasRelevantChanges(@Nonnull Path repoRoot) {
    return hasRelevantChanges(repoRoot, UsageRegistryCiGateProfiles.active());
  }

  public static boolean hasRelevantChanges(
      @Nonnull Path repoRoot, @Nonnull UsageRegistryCiGateProfile profile) {
    if ("true".equals(System.getProperty("restli.usage.check.force"))) {
      return true;
    }
    return GitChangeDetector.hasChanges(
        repoRoot, path -> UsageRegistryGateChangeMatcher.isRestLiGatePath(path, profile));
  }
}
