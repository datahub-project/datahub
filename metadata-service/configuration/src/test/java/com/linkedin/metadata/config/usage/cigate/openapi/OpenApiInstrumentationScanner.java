package com.linkedin.metadata.config.usage.cigate.openapi;

import com.linkedin.metadata.config.usage.cigate.HandlerInstrumentationSurface;
import com.linkedin.metadata.config.usage.cigate.RequestContextInstrumentationScanner;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfile;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

public final class OpenApiInstrumentationScanner {

  private static final String BUILD_OPENAPI_MARKER = "buildOpenapi(";
  private static final Pattern BUILD_OPENAPI = Pattern.compile("\\.buildOpenapi\\(");
  private static final List<String> KNOWN_WRAPPER_METHODS =
      List.of("openapiUsage(", ".usageOpenapi(");

  private OpenApiInstrumentationScanner() {}

  @Nonnull
  public static HandlerInstrumentationSurface scan(@Nonnull Path repoRoot) throws IOException {
    return scan(repoRoot, UsageRegistryCiGateProfiles.active());
  }

  @Nonnull
  public static HandlerInstrumentationSurface scan(
      @Nonnull Path repoRoot, @Nonnull UsageRegistryCiGateProfile profile) throws IOException {
    return RequestContextInstrumentationScanner.scan(
        repoRoot,
        profile.openApiSourceRootSuffixes(),
        BUILD_OPENAPI_MARKER,
        BUILD_OPENAPI,
        KNOWN_WRAPPER_METHODS);
  }
}
