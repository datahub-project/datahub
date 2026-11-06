package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.migration.ClusterMigrationCopy;
import com.linkedin.datahub.upgrade.system.elasticsearch.migration.ClusterMigrationPrepare;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Wiring for the non-entity index family migration (graph, timeseries, system metadata, usage).
 * Both upgrades always exist; each family's step skips itself unless {@code
 * elasticsearch.clusterMigration.<family>.targetCluster} is set, since a Spring property condition
 * cannot express "any family configured".
 */
@Configuration
public class ClusterMigrationConfig {

  @Order(3)
  @Bean(name = "clusterMigrationPrepare")
  @Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
  public BlockingSystemUpgrade clusterMigrationPrepare(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final SearchClusterRegistry searchClusterRegistry,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
          final IndexConvention indexConvention,
      final GraphService graphService,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final GitVersion gitVersion,
      @Qualifier("revision") final String revision) {
    return new ClusterMigrationPrepare(
        opContext,
        entityService,
        searchClusterRegistry,
        indexConvention,
        graphService,
        systemMetadataService,
        timeseriesAspectService,
        upgradeVersion(gitVersion, revision));
  }

  @Order(5)
  @Bean(name = "clusterMigrationCopy")
  @Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
  public NonBlockingSystemUpgrade clusterMigrationCopy(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final SearchClusterRegistry searchClusterRegistry,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
          final IndexConvention indexConvention,
      final GraphService graphService,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final GitVersion gitVersion,
      @Qualifier("revision") final String revision) {
    return new ClusterMigrationCopy(
        opContext,
        entityService,
        searchClusterRegistry,
        indexConvention,
        graphService,
        systemMetadataService,
        timeseriesAspectService,
        upgradeVersion(gitVersion, revision));
  }

  private static String upgradeVersion(final GitVersion gitVersion, final String revision) {
    return String.format("%s-%s", gitVersion.getVersion(), revision);
  }
}
