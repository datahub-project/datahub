package com.linkedin.metadata.config.usage.cigate.restli;

import com.linkedin.metadata.config.usage.cigate.HandlerInstrumentationSurface;
import com.linkedin.metadata.config.usage.cigate.RequestContextInstrumentationScanner;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfile;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

public final class RestliInstrumentationScanner {

  private static final String BUILD_RESTLI_MARKER = "buildRestli(";
  private static final Pattern BUILD_RESTLI = Pattern.compile("\\.buildRestli\\(");

  private RestliInstrumentationScanner() {}

  @Nonnull
  public static HandlerInstrumentationSurface scan(@Nonnull Path repoRoot) throws IOException {
    return scan(repoRoot, UsageRegistryCiGateProfiles.active());
  }

  @Nonnull
  public static HandlerInstrumentationSurface scan(
      @Nonnull Path repoRoot, @Nonnull UsageRegistryCiGateProfile profile) throws IOException {
    return RequestContextInstrumentationScanner.scan(
        repoRoot, profile.restLiSourceRootSuffixes(), BUILD_RESTLI_MARKER, BUILD_RESTLI, List.of());
  }
}
