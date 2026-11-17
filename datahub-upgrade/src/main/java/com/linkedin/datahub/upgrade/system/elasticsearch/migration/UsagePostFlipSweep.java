package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.CopyMode;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Finishes a usage migration after the operator points the family at the other cluster (the flip,
 * or a rollback). The cluster that served until then may hold events that arrived between the last
 * mirrored write and the restart of the pods. The row of the mirror that just ended, now the
 * reverse of the configured pair, names its live staging index on the cluster that serves now and
 * the write index of the other cluster to sweep from: that index and every generation after it are
 * copied into the staging index with create semantics, and the staging index becomes a historical
 * member of the serving alias or data stream unless the copy already made it one. It stays
 * writable, since a rollback mirrors into it again.
 */
@Slf4j
final class UsagePostFlipSweep {
  private static final int SWEEP_PARTITION_BASE = 2_000_000;
  private static final String PENDING = "pending";
  private static final String DONE = "done";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final IndexConvention indexConvention;
  private final String stepId;

  UsagePostFlipSweep(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final IndexConvention indexConvention,
      @Nonnull final String stepId) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.indexConvention = indexConvention;
    this.stepId = stepId;
  }

  /** The row of the migration that just ended, while its sweep is still owed. */
  @Nonnull
  Optional<ClusterMigrationRow> pending(
      @Nonnull final SearchClusterRegistry.FamilyMigration migration) {
    final String endedId =
        ClusterMigrationState.upgradeId(
            SearchComponent.USAGE, migration.getTargetCluster(), migration.getSourceCluster());
    final ClusterMigrationRow ended = ClusterMigrationRow.load(opContext, entityService, endedId);
    if (!ended.existed()
        || !PENDING.equals(ended.get(ClusterMigrationState.USAGE_POST_FLIP_SWEEP))) {
      return Optional.empty();
    }
    if (ended.get(ClusterMigrationState.USAGE_LIVE_STAGING_INDEX) == null
        || ended.get(ClusterMigrationState.USAGE_LAST_SOURCE_WRITE_INDEX) == null) {
      log.warn(
          "{}: row {} owes a usage sweep but does not say which indices; nothing swept",
          stepId,
          endedId);
      return Optional.empty();
    }
    return Optional.of(ended);
  }

  /**
   * @param ended the row {@link #pending} returned
   * @param nowServing the cluster usage is served from now, the ended mirror's target
   * @param nowTarget the cluster usage was served from, the configured mirror's target
   */
  void run(
      @Nonnull final UpgradeContext context,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final ClusterMigrationRow ended,
      @Nonnull final SearchClusterRegistry.ClusterConnection nowServing,
      @Nonnull final SearchClusterRegistry.ClusterConnection nowTarget)
      throws Exception {
    final String liveStaging = ended.get(ClusterMigrationState.USAGE_LIVE_STAGING_INDEX);
    final String lastWriteIndex = ended.get(ClusterMigrationState.USAGE_LAST_SOURCE_WRITE_INDEX);
    log.info(
        "{}: sweep of {} from cluster '{}' into {}",
        stepId,
        lastWriteIndex,
        nowTarget.getName(),
        liveStaging);
    final ClusterHttp oldCluster = new ClusterHttp(opContext, nowTarget.getClient());
    final CrossClusterCopier copier =
        CopierFactory.create(
            opContext,
            CopyMode.IN_PROCESS,
            migration.getSettings().getCopy(),
            nowTarget,
            nowServing,
            ended.getUrn().getId());
    int i = 0;
    for (String backing : indicesToSweep(oldCluster, migration, lastWriteIndex)) {
      if (!oldCluster.indexExists(backing)) {
        log.warn(
            "{}: {} is gone from cluster '{}' (rolled out by its lifecycle policy?); its events"
                + " cannot be swept",
            stepId,
            backing,
            nowTarget.getName());
        continue;
      }
      final CrossClusterCopier.Result result =
          copier.copy(
              new CopyPartition(
                  SWEEP_PARTITION_BASE + i++,
                  backing,
                  liveStaging,
                  "sweep " + backing,
                  null,
                  -1,
                  null),
              null,
              checkpoint -> {},
              () -> false);
      if (!result.isComplete()) {
        throw new IOException("Sweep of " + backing + " was incomplete: " + result);
      }
      context
          .report()
          .addLine(stepId + ": sweep copied " + result.created() + " events from " + backing);
    }
    adoptIntoServingLayout(nowServing, liveStaging);
    ended.put(ClusterMigrationState.USAGE_POST_FLIP_SWEEP, DONE);
    ended.persist(ended.upgradeState());
  }

  /**
   * The old cluster's write index when the mirror ended and every generation after it. Generations
   * sort by name on both layouts ({@code -000004}, {@code .ds-...-2026.10.01-000007}), so the write
   * index need not exist any more to place the ones after it: a lifecycle policy may have removed
   * it during a long bake. Adopted staging members are skipped: they hold the history of the
   * cluster that serves now, and only the old cluster's own generations can carry events it
   * received after the mirror ended.
   */
  @Nonnull
  List<String> indicesToSweep(
      @Nonnull final ClusterHttp oldCluster,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final String lastWriteIndex)
      throws IOException {
    // The other cluster is the configured mirror target now, so its alias carries the target token.
    final String oldLogical =
        migration.getNames().targetToken(opContext) + Constants.DATAHUB_USAGE_EVENT_INDEX;
    final SourceIndexSet oldSet =
        IndexLayoutResolver.resolve(oldCluster, SearchComponent.USAGE, oldLogical);
    final List<String> toSweep = new ArrayList<>();
    if (oldSet != null) {
      for (String backing : oldSet.backingIndices()) {
        if (backing.compareTo(lastWriteIndex) >= 0
            && !MigrationIndexNames.isUsageStagingIndex(backing)) {
          toSweep.add(backing);
        }
      }
    }
    if (toSweep.isEmpty()) {
      toSweep.add(lastWriteIndex);
    }
    return toSweep;
  }

  private void adoptIntoServingLayout(
      @Nonnull final SearchClusterRegistry.ClusterConnection nowServing,
      @Nonnull final String liveStaging)
      throws IOException {
    final ClusterHttp servingCluster = new ClusterHttp(opContext, nowServing.getClient());
    final String servingLogical =
        indexConvention.getIndexName(
            opContext, SearchComponent.USAGE, Constants.DATAHUB_USAGE_EVENT_INDEX);
    final SourceIndexSet servingSet =
        IndexLayoutResolver.resolve(servingCluster, SearchComponent.USAGE, servingLogical);
    final StagingIndexAdopter adopter = new StagingIndexAdopter(servingCluster);
    if (servingSet == null || !servingSet.isRollover()) {
      log.warn(
          "{}: {} on cluster '{}' is a concrete index, so {} keeps the mirrored events but cannot"
              + " join it",
          stepId,
          servingLogical,
          nowServing.getName(),
          liveStaging);
    } else if (servingSet.backingIndices().contains(liveStaging)) {
      adopter.settle(servingSet.layout(), liveStaging, true);
    } else {
      adopter.adopt(servingSet.layout(), servingLogical, liveStaging, true);
    }
  }
}
