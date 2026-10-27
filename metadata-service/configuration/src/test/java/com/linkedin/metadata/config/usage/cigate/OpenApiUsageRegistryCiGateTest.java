package com.linkedin.metadata.config.usage.cigate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.cigate.openapi.OpenApiInstrumentationScanner;
import com.linkedin.metadata.config.usage.cigate.openapi.OpenApiUsageChangeDetector;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class OpenApiUsageRegistryCiGateTest {

  private Path repoRoot;
  private UsageRegistryCiGateProfile profile;

  @BeforeClass
  public void setUp() {
    repoRoot = UsageRegistryRepoPaths.repoRoot();
    profile = UsageRegistryCiGateProfiles.active();
  }

  @Test
  public void testNewOpenApiHandlersAreInstrumented() throws Exception {
    if (!OpenApiUsageChangeDetector.hasRelevantChanges(repoRoot, profile)) {
      throw new SkipException(
          "Skipping OpenAPI usage instrumentation check — no controller source changes detected");
    }
    HandlerInstrumentationSurface current = OpenApiInstrumentationScanner.scan(repoRoot, profile);
    HandlerExemptionSnapshot exemptions = loadExemptions();
    List<HandlerInstrumentationSurface.HandlerEntry> untagged =
        current.uninstrumentedWithoutExemption(exemptions, profile::isOpenApiHandlerExempt);
    Assert.assertTrue(
        untagged.isEmpty(),
        "Uninstrumented OpenAPI handlers without documented exemption:\n"
            + formatFailures(untagged));
  }

  private HandlerExemptionSnapshot loadExemptions() throws Exception {
    ObjectMapper mapper = UsageYamlMapper.create();
    return UsageRegistrySnapshotLoader.loadOpenApiExemptions(
        getClass().getClassLoader(), mapper, profile);
  }

  private static String formatFailures(List<HandlerInstrumentationSurface.HandlerEntry> untagged) {
    List<String> lines = new ArrayList<>();
    for (HandlerInstrumentationSurface.HandlerEntry handler : untagged) {
      lines.add(
          handler.sourceFile()
              + ":"
              + handler.lineNumber()
              + " — add .withUsageOperation(UsageOperation.X) to RequestContext.builder()"
              + ".buildOpenapi(...) chain, or add an entry to openapi_usage_exemptions.snapshot.yaml");
    }
    return String.join("\n", lines);
  }
}
