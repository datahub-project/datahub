package com.linkedin.metadata.config.usage.cigate.graphql;

import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphqlExemptionSnapshotTest {

  private GraphqlUsageCoverageClassifier classifier;

  @BeforeClass
  public void setUp() {
    classifier =
        GraphqlUsageCoverageClassifier.fromBundledYaml(
            new UsageOperationsLoader(UsageYamlMapper.create()));
  }

  @Test
  public void testReconcileDropsExemptionWhenSurfaceBecomesClassified() {
    GraphqlUsageSurface surface =
        new GraphqlUsageSurface(java.util.Set.of("search"), java.util.Set.of(), java.util.Set.of());
    GraphqlExemptionSnapshot existing =
        new GraphqlExemptionSnapshot(
            java.util.List.of(
                new GraphqlExemptionSnapshot.Exemption(
                    "search",
                    GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD,
                    "was unclassified")));
    GraphqlExemptionSnapshot reconciled =
        GraphqlExemptionSnapshot.reconcile(surface, existing, classifier);
    Assert.assertTrue(reconciled.exemptions().isEmpty());
  }

  @Test
  public void testExemptionAllowsUnaccountedSurfaceEntry() {
    GraphqlExemptionSnapshot exemptions =
        new GraphqlExemptionSnapshot(
            java.util.List.of(
                new GraphqlExemptionSnapshot.Exemption(
                    "_internalQuery",
                    GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD,
                    "internal only")));
    GraphqlUsageSurface.GraphqlSurfaceEntry entry =
        new GraphqlUsageSurface.GraphqlSurfaceEntry(
            "_internalQuery", GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD);
    Assert.assertTrue(exemptions.isExempt(entry));
    Assert.assertTrue(
        classifier.formatUnaccountedFailures(java.util.List.of(entry), exemptions).isEmpty());
  }
}
