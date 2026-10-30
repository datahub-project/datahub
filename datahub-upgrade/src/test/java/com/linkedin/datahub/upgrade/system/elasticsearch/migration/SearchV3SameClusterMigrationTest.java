package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

/** The cutover with both generations on one OpenSearch cluster: the default deployment shape. */
public class SearchV3SameClusterMigrationTest extends SearchV3MigrationScenarios {

  @Override
  protected SearchV3MigrationHarness.Topology topology() {
    return SearchV3MigrationHarness.Topology.SAME_CLUSTER;
  }
}
