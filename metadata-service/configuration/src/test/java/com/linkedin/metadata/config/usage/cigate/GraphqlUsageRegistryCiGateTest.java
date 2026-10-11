package com.linkedin.metadata.config.usage.cigate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlExemptionSnapshot;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageChangeDetector;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageCoverageClassifier;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageSurface;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageSurfaceExtractor;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import java.nio.file.Path;
import java.util.List;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphqlUsageRegistryCiGateTest {

  private Path repoRoot;
  private UsageRegistryCiGateProfile profile;
  private GraphqlUsageCoverageClassifier classifier;

  @BeforeClass
  public void setUp() {
    repoRoot = UsageRegistryRepoPaths.repoRoot();
    profile = UsageRegistryCiGateProfiles.active();
    UsageOperationsLoader loader = new UsageOperationsLoader(UsageYamlMapper.create());
    classifier = GraphqlUsageCoverageClassifier.fromBundledYaml(loader);
  }

  @Test
  public void testGraphqlSurfaceIsUsageClassifiedOrExempt() throws Exception {
    if (!GraphqlUsageChangeDetector.hasRelevantChanges(repoRoot, profile)) {
      throw new SkipException(
          "Skipping GraphQL usage surface check — no .graphql file changes detected");
    }
    GraphqlUsageSurface current = GraphqlUsageSurfaceExtractor.extract(repoRoot, profile);
    GraphqlExemptionSnapshot exemptions = loadExemptions();
    List<String> failures = classifier.formatUnaccountedFailures(current.allEntries(), exemptions);
    Assert.assertTrue(
        failures.isEmpty(),
        "GraphQL surface lacks usage_operations.yaml classification:\n"
            + String.join("\n", failures));
  }

  private GraphqlExemptionSnapshot loadExemptions() throws Exception {
    ObjectMapper mapper = UsageYamlMapper.create();
    return UsageRegistrySnapshotLoader.loadGraphqlExemptions(
        getClass().getClassLoader(), mapper, profile);
  }
}
