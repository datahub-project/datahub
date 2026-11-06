package com.linkedin.gms.factory.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.entity.upgrade.EntityClientUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.MirrorLiveMarkerScheduler;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.UpgradeRowMirrorLiveMarker;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.UpgradeRowPoll;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.UpgradeRowWriterHeartbeat;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Gives every configured family migration its mirror-live marker, makes every live writer report
 * itself while a second cluster is declared, and starts polling. The registry builds the mirrors
 * before the system operation context exists (the context is stamped with the registry), so the
 * markers, which read and persist through that context, are created here. The upgrade result store
 * goes through the system entity client so the same wiring serves GMS, the standalone MAE consumer
 * and system-update.
 */
@Slf4j
@Configuration
public class ClusterMigrationMirrorLiveMarkerFactory {

  @Bean(name = "clusterMigrationMirrorLiveMarkers")
  @Nonnull
  public MirrorLiveMarkerScheduler clusterMigrationMirrorLiveMarkers(
      @Qualifier("searchClusterRegistry") final SearchClusterRegistry registry,
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      final ConfigurationProvider configurationProvider,
      @Value("${MAE_CONSUMER_ENABLED:false}") final String maeConsumerEnabled,
      @Value("${MCL_CONSUMER_ENABLED:false}") final String mclConsumerEnabled) {
    final ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    final Set<ClusterMigrationState.Process> roles =
        processRoles(
            esConfig.getClusterMigration().getProcessRole(),
            PreProcessHooks.isMclConsumerEnabled(maeConsumerEnabled, mclConsumerEnabled));
    final DataHubUpgradeResultStore store = new EntityClientUpgradeResultStore(systemEntityClient);
    final List<UpgradeRowPoll> polls = new ArrayList<>();
    // A live writer reports whether or not it migrates anything: the pods a migration's copy must
    // wait out are those of the previous rollout, whose configuration knows no migration. A single
    // cluster can migrate nothing, so such deployments report nothing; system update is not a
    // live writer.
    if (!roles.contains(ClusterMigrationState.Process.UPGRADE)
        && declaresASecondCluster(esConfig)) {
      final UpgradeRowWriterHeartbeat heartbeat =
          new UpgradeRowWriterHeartbeat(
              store,
              systemOpContext,
              BootstrapStep.getUpgradeUrn(ClusterMigrationState.WRITERS_UPGRADE_ID),
              UpgradeRowWriterHeartbeat.localInstance());
      polls.add(heartbeat);
      log.info(
          "Reporting this process ({}) as live writer '{}' on {}",
          roles,
          heartbeat.getInstance(),
          heartbeat.getUpgradeUrn());
    }
    for (SearchClusterRegistry.FamilyMigration migration : registry.familyMigrations()) {
      final Urn upgradeUrn =
          BootstrapStep.getUpgradeUrn(
              ClusterMigrationState.upgradeId(
                  migration.getFamily(),
                  migration.getSourceCluster(),
                  migration.getTargetCluster()));
      polls.add(
          new UpgradeRowMirrorLiveMarker(
              store, systemOpContext, upgradeUrn, roles, migration.getWriteLog()));
      log.info(
          "Mirroring {} writes from cluster '{}' to '{}' as {} (row {})",
          migration.getFamily().getConfigKey(),
          migration.getSourceCluster(),
          migration.getTargetCluster(),
          roles,
          upgradeUrn);
    }
    final MirrorLiveMarkerScheduler scheduler = new MirrorLiveMarkerScheduler(polls);
    scheduler.start();
    return scheduler;
  }

  /** Whether more than one cluster has a uri, the precondition of any migration. */
  static boolean declaresASecondCluster(@Nonnull final ElasticSearchConfiguration esConfig) {
    long withUri = 0;
    for (SearchClusterSettings cluster : esConfig.resolvedClusters().values()) {
      if (cluster.getUri() != null && !cluster.getUri().isBlank()) {
        withUri++;
      }
    }
    return withUri >= 2;
  }

  /**
   * The live-writer roles this process plays: its configured role, plus the MAE consumer when a GMS
   * runs the MCL consumer itself ({@code MAE_CONSUMER_ENABLED} or {@code MCL_CONSUMER_ENABLED}, the
   * gate the consumer itself uses), since that GMS is then the process that writes the MAE-driven
   * index updates.
   */
  @Nonnull
  static Set<ClusterMigrationState.Process> processRoles(
      @Nonnull final String role, final boolean maeConsumerEmbedded) {
    final ClusterMigrationState.Process process = processRole(role);
    final Set<ClusterMigrationState.Process> roles = EnumSet.of(process);
    if (process == ClusterMigrationState.Process.GMS && maeConsumerEmbedded) {
      roles.add(ClusterMigrationState.Process.MAE);
    }
    return roles;
  }

  @Nonnull
  static ClusterMigrationState.Process processRole(@Nonnull final String role) {
    for (ClusterMigrationState.Process process : ClusterMigrationState.Process.values()) {
      if (process.getKey().equals(role.toLowerCase(Locale.ROOT))) {
        return process;
      }
    }
    throw new IllegalStateException(
        "elasticsearch.clusterMigration.processRole '"
            + role
            + "' is not one of gms, mae, upgrade");
  }
}
