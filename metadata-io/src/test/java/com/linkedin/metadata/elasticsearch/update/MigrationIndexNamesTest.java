package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import org.testng.annotations.Test;

public class MigrationIndexNamesTest {
  private final OperationContext opContext = mock(OperationContext.class);

  private static IndexConvention convention(
      String deployPrefix, Map<SearchComponent, String> overlays) {
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
        new ConfiguredIndexPrefixResolver(deployPrefix),
        new EntityIndexConfiguration(),
        overlays);
  }

  @Test
  public void testSamePrefixOnBothClustersIsIdentity() {
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(convention("prod", Map.of()), SearchComponent.GRAPH, null);

    assertEquals(names.target(opContext, "prod_graph_service_v1"), "prod_graph_service_v1");
    assertEquals(names.logIndex(opContext), "prod_cluster_migration_log_graph");
  }

  @Test
  public void testTargetOverlayReplacesTheServingPrefix() {
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(
            convention("prod", Map.of()), SearchComponent.GRAPH, "Tenant");

    assertEquals(names.target(opContext, "prod_graph_service_v1"), "tenant_graph_service_v1");
    assertEquals(names.logIndex(opContext), "tenant_cluster_migration_log_graph");
  }

  @Test
  public void testServingOverlayIsStrippedBeforeTheTargetPrefixIsApplied() {
    IndexConvention convention = convention("prod", Map.of(SearchComponent.TIMESERIES, "blue"));
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(convention, SearchComponent.TIMESERIES, null);

    // The serving cluster names timeseries indices with its overlay; the target inherits the
    // deploy-wide prefix because it declares no overlay of its own.
    assertEquals(
        names.target(opContext, "blue_dataset_datasetprofileaspect_v1"),
        "prod_dataset_datasetprofileaspect_v1");
  }

  @Test
  public void testTheLogOnEachClusterCarriesThatClusterPrefix() {
    IndexConvention convention = convention("prod", Map.of(SearchComponent.GRAPH, "blue"));
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(convention, SearchComponent.GRAPH, "tenant");

    assertEquals(names.logIndex(opContext), "tenant_cluster_migration_log_graph");
    assertEquals(names.logIndexOnServing(opContext), "blue_cluster_migration_log_graph");
  }

  @Test
  public void testNoPrefixAnywhereLeavesNamesBare() {
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(
            convention("", Map.of()), SearchComponent.SYSTEM_METADATA, "");

    assertEquals(
        names.target(opContext, "system_metadata_service_v1"), "system_metadata_service_v1");
    assertEquals(names.logIndex(opContext), "cluster_migration_log_systemmetadata");
  }

  @Test
  public void testUsageAliasWritesLandInTheLiveStagingIndex() {
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(convention("prod", Map.of()), SearchComponent.USAGE, null);

    assertEquals(
        names.target(opContext, "prod_datahub_usage_event"),
        "prod_" + MigrationIndexNames.USAGE_LIVE_STAGING);
    // A write addressed to a concrete backing index is not redirected.
    assertEquals(
        names.target(opContext, "prod_datahub_usage_event-000003"),
        "prod_datahub_usage_event-000003");
  }

  @Test
  public void testUsageBackingIndicesGetTheirOwnStagingIndex() {
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(convention("prod", Map.of()), SearchComponent.USAGE, "tgt");

    assertEquals(
        names.usageStagingFor(opContext, "prod_datahub_usage_event-000003"),
        "tgt_datahub_usage_migration-000003");
    assertEquals(
        names.usageStagingFor(opContext, ".ds-prod_datahub_usage_event-2026.10.01-000007"),
        "tgt_datahub_usage_migration-2026.10.01-000007");
    assertEquals(
        names.usageStagingFor(opContext, "prod_datahub_usage_event"),
        "tgt_datahub_usage_migration-legacy");
  }

  @Test
  public void testAdoptedStagingMembersAreToldApartFromTheAliasOwnGenerations() {
    assertTrue(MigrationIndexNames.isUsageStagingIndex("tgt_datahub_usage_migration-live"));
    assertTrue(MigrationIndexNames.isUsageStagingIndex("datahub_usage_migration-000003"));
    assertFalse(MigrationIndexNames.isUsageStagingIndex("tgt_datahub_usage_event-000003"));
    assertFalse(MigrationIndexNames.isUsageStagingIndex("tgt_datahub_usage_event"));
  }
}
