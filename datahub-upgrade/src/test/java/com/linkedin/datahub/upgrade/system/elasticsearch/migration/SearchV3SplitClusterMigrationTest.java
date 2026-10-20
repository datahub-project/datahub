package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

/**
 * The cutover with V2 on one OpenSearch cluster and V3 on another: what {@code
 * elasticsearch.componentCluster.searchV3=secondary} configures.
 */
public class SearchV3SplitClusterMigrationTest extends SearchV3MigrationScenarios {

  @Override
  protected SearchV3MigrationHarness.Topology topology() {
    return SearchV3MigrationHarness.Topology.SEPARATE_CLUSTERS;
  }
}
