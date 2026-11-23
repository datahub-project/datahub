package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.ClusterMigrationConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Blocking half of the non-entity index family migration: one prepare step per migratable family,
 * each of which skips itself unless its family names a target cluster. Runs after {@code
 * BuildIndices} so the serving indices are settled before anything is created on the target.
 */
public class ClusterMigrationPrepare implements BlockingSystemUpgrade {
  public static final String ID = "ClusterMigrationPrepare";

  private final List<UpgradeStep> steps = new ArrayList<>();

  public ClusterMigrationPrepare(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final SearchClusterRegistry registry,
      @Nonnull final IndexConvention indexConvention,
      @Nonnull final GraphService graphService,
      @Nonnull final SystemMetadataService systemMetadataService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final String upgradeVersion) {
    for (SearchComponent component : ClusterMigrationConfiguration.MIGRATABLE_FAMILIES) {
      steps.add(
          new ClusterMigrationPrepareStep(
              opContext,
              entityService,
              registry,
              indexConvention,
              MigrationFamily.of(
                  component, graphService, systemMetadataService, timeseriesAspectService),
              upgradeVersion));
    }
  }

  @Override
  public String id() {
    return ID;
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
