package com.linkedin.metadata.config.usage.cigate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.cigate.restli.RestliInstrumentationScanner;
import com.linkedin.metadata.config.usage.cigate.restli.RestliUsageChangeDetector;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RestliUsageRegistryCiGateTest {

  private Path repoRoot;
  private UsageRegistryCiGateProfile profile;

  @BeforeClass
  public void setUp() {
    repoRoot = UsageRegistryRepoPaths.repoRoot();
    profile = UsageRegistryCiGateProfiles.active();
  }

  @Test
  public void testRestLiHandlersAreInstrumentedOrExempt() throws Exception {
    if (!RestliUsageChangeDetector.hasRelevantChanges(repoRoot, profile)) {
      throw new SkipException(
          "Skipping Rest.li usage instrumentation check — no resource source changes detected");
    }
    HandlerInstrumentationSurface current = RestliInstrumentationScanner.scan(repoRoot, profile);
    HandlerExemptionSnapshot exemptions = loadExemptions();
    List<HandlerInstrumentationSurface.HandlerEntry> untagged =
        current.uninstrumentedWithoutExemption(exemptions, profile::isRestLiHandlerExempt);
    Assert.assertTrue(
        untagged.isEmpty(),
        "Uninstrumented Rest.li handlers without documented exemption:\n"
            + formatFailures(untagged));
  }

  private HandlerExemptionSnapshot loadExemptions() throws Exception {
    ObjectMapper mapper = UsageYamlMapper.create();
    return UsageRegistrySnapshotLoader.loadRestLiExemptions(
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
              + ".buildRestli(...) chain, or add an entry to restli_usage_exemptions.snapshot.yaml");
    }
    return String.join("\n", lines);
  }
}
