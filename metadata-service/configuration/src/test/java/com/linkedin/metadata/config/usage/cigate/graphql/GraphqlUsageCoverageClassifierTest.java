package com.linkedin.metadata.config.usage.cigate.graphql;

import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphqlUsageCoverageClassifierTest {

  private GraphqlUsageCoverageClassifier classifier;

  @BeforeClass
  public void setUp() {
    classifier =
        GraphqlUsageCoverageClassifier.fromBundledYaml(
            new UsageOperationsLoader(UsageYamlMapper.create()));
  }

  @Test
  public void testSearchRootFieldIsAccountedFor() {
    var result =
        classifier.classify(
            new GraphqlUsageSurface.GraphqlSurfaceEntry(
                "search", GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD));
    Assert.assertTrue(result.isAccountedFor());
    Assert.assertEquals(result.source(), GraphqlClassificationSource.EXPLICIT_NAME);
  }

  @Test
  public void testDatasetRootFieldUsesEntityReadDefault() {
    var result =
        classifier.classify(
            new GraphqlUsageSurface.GraphqlSurfaceEntry(
                "dataset", GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD));
    Assert.assertTrue(result.isAccountedFor());
    Assert.assertEquals(result.source(), GraphqlClassificationSource.ENTITY_READ_DEFAULT);
  }

  @Test
  public void testFullExtractedSurfaceIsUsageClassified() throws Exception {
    var surface =
        GraphqlUsageSurfaceExtractor.extract(
            com.linkedin.metadata.config.usage.cigate.UsageRegistryRepoPaths.repoRoot());
    var entries = new java.util.ArrayList<GraphqlUsageSurface.GraphqlSurfaceEntry>();
    for (String name : surface.queryRootFields()) {
      entries.add(
          new GraphqlUsageSurface.GraphqlSurfaceEntry(
              name, GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD));
    }
    for (String name : surface.mutationRootFields()) {
      entries.add(
          new GraphqlUsageSurface.GraphqlSurfaceEntry(
              name, GraphqlUsageSurface.GraphqlSurfaceKind.MUTATION_ROOT_FIELD));
    }
    for (String name : surface.clientOperationNames()) {
      entries.add(
          new GraphqlUsageSurface.GraphqlSurfaceEntry(
              name, GraphqlUsageSurface.GraphqlSurfaceKind.CLIENT_OPERATION));
    }
    var failures = classifier.formatUnaccountedFailures(entries);
    Assert.assertTrue(
        failures.isEmpty(),
        "GraphQL surface lacks usage_operations.yaml classification:\n"
            + String.join("\n", failures));
  }
}
