package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexSetup;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.CopyMode;
import com.linkedin.metadata.config.search.CopySettings;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState.Status;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking move of one family's history to the migration target, resumable across runs and
 * deploys: copy the planned partitions, run a windowed insurance pass from the moment every live
 * writer was mirroring, replay the mirrored deletes, adopt staging indices into the target's
 * rollover alias or data stream, and verify. Each phase advances the migration row's status; a time
 * budget or a wait returns SUCCEEDED with the row in place so the rest of the non-blocking run
 * proceeds and the next run resumes here. After a flip the same step, now configured with the
 * clusters the other way round, finishes what the ended migration left: the usage sweep and the
 * removal of its delete log.
 */
@Slf4j
public class ClusterMigrationCopyStep implements UpgradeStep {
  static final String PARTITION_STATUS = "status";
  static final String PARTITION_CURSOR = "cursor";
  static final String PARTITION_READ = "read";
  static final String PARTITION_CREATED = "created";
  static final String PARTITION_PRESENT = "alreadyPresent";
  static final String PARTITION_FAILURES = "failures";
  static final String PARTITION_SPEC = "spec";
  static final String PARTITION_DONE = "DONE";
  static final String PARTITION_FAILED = "FAILED";
  static final String PARTITION_PAUSED = "PAUSED";
  static final String WINDOW_START = ClusterMigrationState.WINDOW_PREFIX + "start";
  private static final int WINDOW_PARTITION_BASE = 1_000_000;
  private static final long LIVE_WRITER_POLL_MS = TimeUnit.SECONDS.toMillis(5);
  private static final int LIVE_WRITER_LOG_EVERY_POLLS = 12;
  private static final int PARTITION_ATTEMPTS = 3;
  private static final long PARTITION_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(10);
  private static final long VERIFY_RECHECK_PAUSE_MS = TimeUnit.SECONDS.toMillis(5);

  /** Between windowed passes while one keeps writing; not final so a test need not wait it out. */
  static long windowRepeatPauseMs = TimeUnit.SECONDS.toMillis(30);

  private static final int MAX_WINDOW_ROUNDS = 20;

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchClusterRegistry registry;
  private final IndexConvention indexConvention;
  private final MigrationFamily family;
  private final String upgradeVersion;
  private final UsagePostFlipSweep usageSweep;

  public ClusterMigrationCopyStep(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final SearchClusterRegistry registry,
      @Nonnull final IndexConvention indexConvention,
      @Nonnull final MigrationFamily family,
      @Nonnull final String upgradeVersion) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.registry = registry;
    this.indexConvention = indexConvention;
    this.family = family;
    this.upgradeVersion = upgradeVersion;
    this.usageSweep = new UsagePostFlipSweep(opContext, entityService, indexConvention, id());
  }

  @Override
  public String id() {
    return "ClusterMigrationCopy_" + family.getFamily().getConfigKey();
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    return registry.migrationFor(family.getFamily()).isEmpty();
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        return run(context);
      } catch (Exception e) {
        log.error("{} failed", id(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Nonnull
  private UpgradeStepResult run(@Nonnull final UpgradeContext context) throws Exception {
    final SearchClusterRegistry.FamilyMigration migration =
        registry.migrationFor(family.getFamily()).orElseThrow();
    family.checkSupported();
    final SearchClusterRegistry.ClusterConnection source =
        registry.connection(migration.getSourceCluster());
    final SearchClusterRegistry.ClusterConnection target =
        registry.targetConnectionFor(family.getFamily());
    final ClusterHttp sourceHttp = new ClusterHttp(opContext, source.getClient());
    final ClusterHttp targetHttp = new ClusterHttp(opContext, target.getClient());
    final String upgradeId =
        ClusterMigrationState.upgradeId(
            family.getFamily(), migration.getSourceCluster(), migration.getTargetCluster());
    final int liveWriterWaitMinutes =
        source.getConfig().getClusterMigration().getLiveWriterWaitMinutes();
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);

    // After a flip or a rollback the old cluster is the mirror target: finish the usage migration
    // that just ended, once every pod writes through the new mirror and nothing reaches the old
    // cluster's alias any more.
    final Optional<ClusterMigrationRow> endedUsage =
        family.getFamily() == SearchComponent.USAGE
            ? usageSweep.pending(migration)
            : Optional.empty();
    if (endedUsage.isPresent()) {
      if (!row.existed()) {
        return noRow(upgradeId);
      }
      row = awaitLiveWriters(row, liveWriterWaitMinutes);
      if (!liveWritersReady(row)) {
        return waitingForLiveWriters(context, row, "the usage sweep");
      }
      usageSweep.run(context, migration, endedUsage.get(), source, target);
    }
    removeVerifiedLogs(migration, row, sourceHttp, targetHttp);

    if (!copyEnabled(row, migration)) {
      log.info("{}: copy disabled for this migration (mirror only); nothing to copy", id());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }
    if (!row.existed()) {
      return noRow(upgradeId);
    }
    Status status = row.status().orElse(Status.PREPARED);
    if (status.isTerminal()) {
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }
    final CopySettings copy = migration.getSettings().getCopy();
    final CopyMode mode = copy.copyMode();
    row = awaitLiveWriters(row, liveWriterWaitMinutes);
    if (!liveWritersReady(row)) {
      return waitingForLiveWriters(context, row, "the copy");
    }
    final Optional<Long> mirroringSince =
        ClusterMigrationState.liveWritersMirroringSince(row.state());
    dropCursorsOfAnotherCopier(row, mode);
    final List<String> logicalNames = family.logicalNames(opContext, indexConvention);
    if (ServingRebuildGuard.inFlight(opContext, entityService, upgradeVersion, logicalNames)) {
      log.info("{}: a serving index is mid-rebuild; resuming on a later run", id());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }

    final List<SourceIndexSet> sourceSets = resolveSources(sourceHttp, logicalNames);
    final Long deadline =
        copy.getMaxHours() > 0
            ? System.currentTimeMillis() + TimeUnit.HOURS.toMillis(copy.getMaxHours())
            : null;
    final BooleanSupplier shouldStop =
        () -> deadline != null && System.currentTimeMillis() >= deadline;
    final CrossClusterCopier copier =
        CopierFactory.create(
            opContext,
            mode == CopyMode.EXTERNAL ? CopyMode.IN_PROCESS : mode,
            copy,
            source,
            target,
            upgradeId);
    final String liveStaging =
        family.getFamily() == SearchComponent.USAGE
            ? migration.getNames().target(opContext, logicalNames.get(0))
            : null;

    if (status == Status.PREPARED || status == Status.COPYING || status == Status.FAILED) {
      if (mode == CopyMode.EXTERNAL) {
        final Long watermark = copy.getExternalWatermarkEpochMs();
        if (watermark == null) {
          log.error("{}: copy.mode=external needs copy.externalWatermarkEpochMs", id());
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        row.put(ClusterMigrationState.COPY_START_TIME, watermark);
        row.put(ClusterMigrationState.COPY_COMPLETE_TIME, System.currentTimeMillis());
        rememberUsageSweepStart(row, sourceSets, liveStaging);
        status = Status.WINDOW;
        row.setStatus(status);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
      } else {
        final List<CopyPartition> partitions =
            planOrLoad(row, migration, sourceHttp, sourceSets, copy, liveStaging);
        status = Status.COPYING;
        row.setStatus(status);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
        ensureStagingIndices(target, partitions);
        final PartitionOutcome outcome =
            runPartitions(row, partitions, copier, parallelism(copy, source, target), shouldStop);
        if (outcome.failed > 0) {
          row.setStatus(Status.FAILED);
          row.persist(DataHubUpgradeState.IN_PROGRESS);
          context.report().addLine(id() + ": " + outcome.failed + " partition(s) failed; rerun");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        if (outcome.paused > 0) {
          row.persist(DataHubUpgradeState.IN_PROGRESS);
          context
              .report()
              .addLine(
                  id() + ": budget reached, " + outcome.paused + " partition(s) resume next run");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }
        row.put(ClusterMigrationState.COPY_COMPLETE_TIME, System.currentTimeMillis());
        status = Status.WINDOW;
        row.setStatus(status);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
      }
    }

    if (status == Status.WINDOW) {
      final long copyStart = row.getLong(ClusterMigrationState.COPY_START_TIME).orElseThrow();
      final long windowStart =
          (mode == CopyMode.EXTERNAL ? copyStart : Math.min(mirroringSince.get(), copyStart))
              - copy.getSkewMs();
      row.put(WINDOW_START, windowStart);
      final CrossClusterCopier windowCopier =
          windowCopier(mode, copier, copy, source, target, upgradeId);
      // A pass that wrote anything found writes the mirror had not delivered: by pods of the
      // previous rollout that were still serving the old configuration when the copy began, or by
      // a mirror failure. Only a pass that writes nothing shows the target current, so the pass
      // repeats until one does; nothing is verified on the strength of a pass that repaired.
      for (int round = 1; ; round++) {
        final PartitionOutcome outcome =
            runWindowPass(row, migration, sourceSets, windowCopier, windowStart, shouldStop);
        if (outcome.failed > 0) {
          row.setStatus(Status.FAILED);
          row.persist(DataHubUpgradeState.IN_PROGRESS);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        if (outcome.paused > 0) {
          row.persist(DataHubUpgradeState.IN_PROGRESS);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }
        final long written = writtenByWindowPass(row);
        if (written == 0) {
          break;
        }
        context
            .report()
            .addLine(
                id()
                    + ": windowed pass "
                    + round
                    + " wrote "
                    + written
                    + " document(s) the mirror had not delivered; running it again");
        forgetWindowPartitions(row);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
        if (round >= MAX_WINDOW_ROUNDS) {
          context
              .report()
              .addLine(
                  id()
                      + ": the windowed pass kept finding undelivered writes for "
                      + MAX_WINDOW_ROUNDS
                      + " rounds; not verified, it resumes on the next run");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        if (shouldStop.getAsBoolean()) {
          context.report().addLine(id() + ": budget reached, the windowed pass resumes next run");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }
        Thread.sleep(windowRepeatPauseMs);
      }
      status = Status.REPLAY;
      row.setStatus(status);
      row.persist(DataHubUpgradeState.IN_PROGRESS);
    }

    if (status == Status.REPLAY) {
      final long from = row.getLong(WINDOW_START).orElseThrow();
      final long copyStart = row.getLong(ClusterMigrationState.COPY_START_TIME).orElseThrow();
      final Map<String, String> sourceIndexByTarget = new HashMap<>();
      for (SourceIndexSet set : sourceSets) {
        sourceIndexByTarget.put(
            migration.getNames().target(opContext, set.logicalName()), set.logicalName());
      }
      final MirroredWriteLogReplayer replayer =
          new MirroredWriteLogReplayer(
              targetHttp,
              sourceHttp,
              sourceIndexByTarget,
              family.isMutable() ? family.timeField() : null);
      final long replayed = replayer.replay(migration.getNames().logIndex(opContext), from);
      // The documents the target received before the copy began were mirrored while the previous
      // rollout's pods were still writing. One of those pods deleting such a document on the
      // source,
      // without the mirror, leaves it on the target alone, and only the source can say so.
      long overlap = 0;
      for (SourceIndexSet set : sourceSets) {
        if (set.isRollover()) {
          continue;
        }
        overlap +=
            replayer.reconcileMatching(
                migration.getNames().target(opContext, set.logicalName()),
                family.windowFilter(opContext.getObjectMapper(), from, copyStart));
      }
      row.put(ClusterMigrationState.REPLAY_WATERMARK, System.currentTimeMillis());
      context
          .report()
          .addLine(
              id()
                  + ": replayed "
                  + replayed
                  + " mirrored delete/by-query entries and reconciled "
                  + overlap
                  + " document(s) the target received before the copy began");
      status = Status.ADOPTING;
      row.setStatus(status);
      row.persist(DataHubUpgradeState.IN_PROGRESS);
    }

    if (status == Status.ADOPTING) {
      adoptStagingIndices(row, migration, targetHttp, liveStaging);
      final int missing =
          new CopyVerifier(opContext, sourceHttp, targetHttp, family, id(), VERIFY_RECHECK_PAUSE_MS)
              .verify(context, row, migration, sourceSets);
      if (missing > 0) {
        // The newest source documents were mirrored and both sides are refreshed, so one the
        // target lacks was lost by the mirror. The windowed pass from the mirror's start restores
        // it from the source, so the row goes back there; nothing is verified, and the delete log
        // stays, until the sample is clean.
        status = Status.WINDOW;
        row.setStatus(status);
        forgetWindowPartitions(row);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
        context
            .report()
            .addLine(
                id()
                    + ": "
                    + missing
                    + " of the newest source documents are missing on the target; not verified,"
                    + " the windowed pass runs again on the next run");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      status = Status.VERIFIED;
      row.setStatus(status);
      row.persist(DataHubUpgradeState.SUCCEEDED);
      context
          .report()
          .addLine(
              id()
                  + ": migration "
                  + upgradeId
                  + " verified; flip componentCluster."
                  + family.getFamily().getConfigKey()
                  + " to '"
                  + migration.getTargetCluster()
                  + "' on the next deploy, which also removes the delete log");
    }
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
  }

  /**
   * The prepare step's decision on the copy, which it may have set aside (nothing is left to copy
   * once the reverse migration has verified); the configuration alone when no prepare has run.
   */
  private static boolean copyEnabled(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration) {
    final String decided = row.get(ClusterMigrationState.COPY_ENABLED);
    return decided == null
        ? migration.getSettings().isCopyEnabled()
        : Boolean.parseBoolean(decided);
  }

  @Nonnull
  private UpgradeStepResult noRow(@Nonnull final String upgradeId) {
    log.error("{}: no migration row {}; the blocking prepare step has not run", id(), upgradeId);
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
  }

  @Nonnull
  private UpgradeStepResult waitingForLiveWriters(
      @Nonnull final UpgradeContext context,
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final String what) {
    log.info(
        "{}: GMS and the MAE consumer have not reported their {} mirror live, or a writer of the"
            + " previous rollout is still reporting (markers: {}; previous rollout's writers: {});"
            + " {} resumes on a later run",
        id(),
        family.getFamily().getConfigKey(),
        recordedMarkers(row),
        previousRolloutWriters(row),
        what);
    context
        .report()
        .addLine(
            id()
                + ": waiting for the live writers' mirror markers and for the previous rollout's"
                + " writers to stop");
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
  }

  /**
   * The copy may not read the source before every live writer mirrors: a write that an old,
   * unmirrored pod made after the copy read the document would be missing on the target. The same
   * holds for the usage sweep after a flip, which must not read the old cluster's write index while
   * an old pod can still write to it. GMS and the MAE consumer record their markers within a minute
   * of starting, and the previous rollout's pods stop reporting within minutes of terminating, so
   * waiting out the rollout lets the non-blocking run of the deploy finish on its own; past the
   * wait the step returns and a later run resumes. The row is reloaded so that what the step
   * persists afterwards keeps the markers.
   */
  @Nonnull
  private ClusterMigrationRow awaitLiveWriters(
      @Nonnull final ClusterMigrationRow row, final int waitMinutes) throws InterruptedException {
    ClusterMigrationRow current = row;
    final long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(waitMinutes);
    int polls = 0;
    while (!liveWritersReady(current) && System.currentTimeMillis() < deadline) {
      if (polls++ % LIVE_WRITER_LOG_EVERY_POLLS == 0) {
        log.info(
            "{}: waiting up to {} min for GMS and the MAE consumer to report their {} mirror live"
                + " and for the previous rollout's writers to stop (markers so far: {}; previous"
                + " rollout's writers still reporting: {})",
            id(),
            waitMinutes,
            family.getFamily().getConfigKey(),
            recordedMarkers(current),
            previousRolloutWriters(current));
      }
      Thread.sleep(LIVE_WRITER_POLL_MS);
      current = ClusterMigrationRow.load(opContext, entityService, row.getUrn().getId());
    }
    return current;
  }

  /**
   * Whether every live writer of this rollout mirrors and none of the previous rollout's is still
   * writing: the GMS and MAE markers are recorded for this rollout, and no writer that started
   * before the row's rollout stamp has reported within the last few minutes. A row without a stamp
   * (a developer stack, the harness) cannot tell the rollouts apart and is gated by the markers
   * alone.
   */
  private boolean liveWritersReady(@Nonnull final ClusterMigrationRow row) {
    return ClusterMigrationState.liveWritersMirroringSince(row.state()).isPresent()
        && previousRolloutWriters(row).isEmpty();
  }

  /** The previous rollout's writers still reporting, by instance; see {@link #liveWritersReady}. */
  @Nonnull
  private List<String> previousRolloutWriters(@Nonnull final ClusterMigrationRow row) {
    final Optional<Long> rolloutStartedAt = row.getLong(ClusterMigrationState.ROLLOUT_STARTED_AT);
    if (rolloutStartedAt.isEmpty()) {
      return List.of();
    }
    final ClusterMigrationRow writers =
        ClusterMigrationRow.load(
            opContext, entityService, ClusterMigrationState.WRITERS_UPGRADE_ID);
    return ClusterMigrationState.previousRolloutWritersReporting(
            writers.state(), rolloutStartedAt.get(), System.currentTimeMillis())
        .stream()
        .map(ClusterMigrationState.Writer::instance)
        .toList();
  }

  /**
   * The delete log of a copy that has verified is removed by the run after the verification, not by
   * the verifying run: the live writers stop logging once their minute poll sees the row verified,
   * and a log deleted under them would be created again by a late entry. After a flip the verified
   * migration is the reverse of the configured pair and its log sits on the cluster that serves
   * now.
   */
  private void removeVerifiedLogs(
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final ClusterHttp sourceHttp,
      @Nonnull final ClusterHttp targetHttp)
      throws IOException {
    if (row.status().map(Status::isTerminal).orElse(false)) {
      removeLog(targetHttp, migration.getNames().logIndex(opContext), migration.getTargetCluster());
    }
    final ClusterMigrationRow ended =
        ClusterMigrationRow.load(
            opContext,
            entityService,
            ClusterMigrationState.upgradeId(
                family.getFamily(), migration.getTargetCluster(), migration.getSourceCluster()));
    if (ended.status().map(Status::isTerminal).orElse(false)) {
      removeLog(
          sourceHttp,
          migration.getNames().logIndexOnServing(opContext),
          migration.getSourceCluster());
    }
  }

  private void removeLog(
      @Nonnull final ClusterHttp cluster, @Nonnull final String logIndex, final String clusterName)
      throws IOException {
    if (cluster.indexExists(logIndex)) {
      log.info(
          "{}: removing the delete log {} of a verified copy from '{}'",
          id(),
          logIndex,
          clusterName);
      cluster.deleteIndex(logIndex);
    }
  }

  /**
   * A cursor belongs to the copier that wrote it: a remote reindex task id means nothing to a
   * scroll, and a scroll id nothing to a task lookup. When the configured mode is not the one the
   * row's partitions last ran with, every cursor is dropped and a paused partition forgets its
   * pause, so each unfinished partition starts over, which create semantics make safe, and the row
   * records the mode now in use.
   */
  private void dropCursorsOfAnotherCopier(
      @Nonnull final ClusterMigrationRow row, @Nonnull final CopyMode mode) {
    final String configured = mode.getConfigValue();
    final String recorded = row.get(ClusterMigrationState.COPY_MODE);
    if (configured.equals(recorded)) {
      return;
    }
    if (recorded != null) {
      int dropped = 0;
      for (String key : new ArrayList<>(row.state().keySet())) {
        if (!key.startsWith(ClusterMigrationState.PARTITION_PREFIX)) {
          continue;
        }
        if (key.endsWith("." + PARTITION_CURSOR)) {
          row.remove(key);
          dropped++;
        } else if (key.endsWith("." + PARTITION_STATUS) && PARTITION_PAUSED.equals(row.get(key))) {
          row.remove(key);
        }
      }
      log.info(
          "{}: copy mode changed from {} to {}; {} partition cursor(s) dropped, those partitions"
              + " start over",
          id(),
          recorded,
          configured,
          dropped);
    }
    row.put(ClusterMigrationState.COPY_MODE, configured);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
  }

  @Nonnull
  private static List<String> recordedMarkers(@Nonnull final ClusterMigrationRow row) {
    return row.state().entrySet().stream()
        .filter(e -> e.getKey().startsWith(ClusterMigrationState.MIRROR_ACTIVE_SINCE_PREFIX))
        .map(e -> e.getKey() + "=" + e.getValue())
        .toList();
  }

  // ---- planning ------------------------------------------------------------------------------

  @Nonnull
  private List<SourceIndexSet> resolveSources(
      @Nonnull final ClusterHttp sourceHttp, @Nonnull final List<String> logicalNames)
      throws IOException {
    final List<SourceIndexSet> sets = new ArrayList<>();
    for (String name : logicalNames) {
      final SourceIndexSet set = IndexLayoutResolver.resolve(sourceHttp, family.getFamily(), name);
      if (set == null) {
        log.info("{}: {} does not exist on the source; nothing to copy for it", id(), name);
        continue;
      }
      sets.add(set);
    }
    return sets;
  }

  @Nonnull
  private List<CopyPartition> planOrLoad(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final ClusterHttp sourceHttp,
      @Nonnull final List<SourceIndexSet> sourceSets,
      @Nonnull final CopySettings copy,
      @Nullable final String liveStaging)
      throws IOException {
    final Optional<Long> count = row.getLong(ClusterMigrationState.PARTITION_COUNT);
    if (count.isPresent()) {
      return loadPartitions(row, count.get());
    }
    final List<CopyPartition> partitions =
        new PartitionPlanner(sourceHttp, copy.getTargetDocsPerPartition())
            .plan(
                family.getFamily(),
                sourceSets,
                name ->
                    liveStaging != null
                        ? migration.getNames().usageStagingFor(opContext, name)
                        : migration.getNames().target(opContext, name),
                liveStaging);
    for (CopyPartition partition : partitions) {
      row.put(
          ClusterMigrationState.partitionKey(partition.id(), PARTITION_SPEC),
          partition.toJson(opContext.getObjectMapper()));
    }
    row.put(ClusterMigrationState.PARTITION_COUNT, partitions.size());
    row.put(ClusterMigrationState.COPY_START_TIME, System.currentTimeMillis());
    rememberUsageSweepStart(row, sourceSets, liveStaging);
    log.info(
        "{}: planned {} partitions covering {} documents",
        id(),
        partitions.size(),
        partitions.stream().mapToLong(CopyPartition::estimatedDocs).sum());
    return partitions;
  }

  @Nonnull
  private List<CopyPartition> loadPartitions(
      @Nonnull final ClusterMigrationRow row, final long count) {
    final List<CopyPartition> partitions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      partitions.add(
          CopyPartition.fromJson(
              opContext.getObjectMapper(),
              row.get(ClusterMigrationState.partitionKey(i, PARTITION_SPEC))));
    }
    return partitions;
  }

  /**
   * Where the usage sweep after the flip starts: the source's write index when the copy began,
   * since every event that reaches it or a later generation from now on is mirrored into the live
   * staging index, and the sweep carries over whatever arrived after the mirror ended.
   */
  private static void rememberUsageSweepStart(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final List<SourceIndexSet> sourceSets,
      @Nullable final String liveStaging) {
    if (liveStaging == null) {
      return;
    }
    for (SourceIndexSet set : sourceSets) {
      row.put(ClusterMigrationState.USAGE_LAST_SOURCE_WRITE_INDEX, set.writeIndex());
      row.put(ClusterMigrationState.USAGE_LIVE_STAGING_INDEX, liveStaging);
    }
  }

  private void ensureStagingIndices(
      @Nonnull final SearchClusterRegistry.ClusterConnection target,
      @Nonnull final List<CopyPartition> partitions)
      throws Exception {
    if (family.getFamily() != SearchComponent.USAGE) {
      return;
    }
    final int numShards = target.getConfig().getIndex().getNumShards();
    final int numReplicas = target.getConfig().getIndex().getNumReplicas();
    final RefreshIntervals intervals = target.getConfig().getIndex().getRefreshIntervals();
    final int refresh =
        intervals == null || intervals.getUsageSeconds() == null ? 30 : intervals.getUsageSeconds();
    final boolean openSearch = target.getClient().getEngineType().isOpenSearch();
    for (CopyPartition partition : partitions) {
      UsageEventIndexSetup.createStagingIndex(
          opContext,
          target.asComponents(indexConvention),
          openSearch,
          partition.targetIndex(),
          numShards,
          numReplicas,
          refresh);
    }
  }

  // ---- copying -------------------------------------------------------------------------------

  private record PartitionOutcome(int done, int paused, int failed) {}

  /**
   * Partitions run at once: the configured parallelism, bounded by each cluster's connection pool.
   * A running partition holds one connection on each side for the whole of a page, and a partition
   * waiting for a connection beyond the client's request timeout fails.
   */
  private int parallelism(
      @Nonnull final CopySettings copy,
      @Nonnull final SearchClusterRegistry.ClusterConnection source,
      @Nonnull final SearchClusterRegistry.ClusterConnection target) {
    final int sourcePool = poolOf(source);
    final int targetPool = poolOf(target);
    final int bounded =
        Math.max(1, Math.min(copy.getParallelism(), Math.min(sourcePool, targetPool)));
    if (bounded < copy.getParallelism()) {
      log.warn(
          "{}: copy.parallelism {} exceeds the connection pool of cluster '{}' ({}) or '{}' ({});"
              + " running {} partitions at once. Raise threadCount on the clusters for more.",
          id(),
          copy.getParallelism(),
          source.getName(),
          sourcePool,
          target.getName(),
          targetPool,
          bounded);
    }
    return bounded;
  }

  /** The connection pool a cluster's client keeps, which bounds what can run against it at once. */
  private static int poolOf(@Nonnull final SearchClusterRegistry.ClusterConnection connection) {
    return SearchClientShim.ShimConfiguration.maxConnectionsPerRoute(
        connection.getClient().getShimConfiguration().getThreadCount());
  }

  @Nonnull
  private PartitionOutcome runPartitions(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final List<CopyPartition> partitions,
      @Nonnull final CrossClusterCopier copier,
      final int parallelism,
      @Nonnull final BooleanSupplier shouldStop)
      throws InterruptedException {
    final ExecutorService pool =
        Executors.newFixedThreadPool(
            Math.max(1, parallelism),
            r -> {
              final Thread t =
                  new Thread(r, "cluster-migration-" + family.getFamily().getConfigKey());
              t.setDaemon(true);
              return t;
            });
    final List<Future<String>> futures = new ArrayList<>();
    try {
      for (CopyPartition partition : partitions) {
        if (PARTITION_DONE.equals(
            row.get(ClusterMigrationState.partitionKey(partition.id(), PARTITION_STATUS)))) {
          continue;
        }
        futures.add(
            pool.submit(
                () ->
                    // Past the budget nothing more is started: an untouched partition is simply
                    // left for the next run, instead of opening a scroll to copy one page.
                    shouldStop.getAsBoolean()
                        ? PARTITION_PAUSED
                        : runPartition(row, partition, copier, shouldStop)));
      }
      int done = 0;
      int paused = 0;
      int failed = 0;
      for (Future<String> future : futures) {
        final String outcome;
        try {
          outcome = future.get();
        } catch (ExecutionException e) {
          log.error("{}: partition failed", id(), e.getCause());
          failed++;
          continue;
        }
        if (PARTITION_DONE.equals(outcome)) {
          done++;
        } else if (PARTITION_PAUSED.equals(outcome)) {
          paused++;
        } else {
          failed++;
        }
      }
      return new PartitionOutcome(done, paused, failed);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Runs one partition to its end, a pause or a failure. Only a partition that paused resumes from
   * its checkpoint: a pause checkpoints right after a page was written and fetches nothing more, so
   * the cursor is exactly where the copy stopped. Anything else (a failed partition, a run that
   * died mid-page) starts the partition over, which the write semantics make safe.
   */
  @Nonnull
  private String runPartition(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final CopyPartition partition,
      @Nonnull final CrossClusterCopier copier,
      @Nonnull final BooleanSupplier shouldStop) {
    final String cursorKey = ClusterMigrationState.partitionKey(partition.id(), PARTITION_CURSOR);
    final String statusKey = ClusterMigrationState.partitionKey(partition.id(), PARTITION_STATUS);
    final CrossClusterCopier.Checkpoint resumeFrom = claimPartition(row, partition);
    final CrossClusterCopier.Checkpointer checkpointer =
        checkpoint -> {
          synchronized (row) {
            if (checkpoint.cursor() != null) {
              row.put(cursorKey, checkpoint.cursor());
            }
            putCounts(
                row,
                partition.id(),
                checkpoint.read(),
                checkpoint.created(),
                checkpoint.alreadyPresent(),
                checkpoint.failures());
            row.persist(DataHubUpgradeState.IN_PROGRESS);
          }
        };
    try {
      final CrossClusterCopier.Result result =
          copyWithRetries(
              partition, resumeFrom, copier, checkpointer, shouldStop, PARTITION_RETRY_BACKOFF_MS);
      synchronized (row) {
        putCounts(
            row,
            partition.id(),
            result.read(),
            result.created(),
            result.alreadyPresent(),
            result.failures());
        final String status;
        if (result.paused()) {
          status = PARTITION_PAUSED;
        } else if (result.isComplete()) {
          status = PARTITION_DONE;
          row.remove(cursorKey);
        } else {
          status = PARTITION_FAILED;
          row.remove(cursorKey);
          log.error(
              "{}: partition {} ({}) incomplete: read {}, created {}, present {}, failures {}",
              id(),
              partition.id(),
              partition.label(),
              result.read(),
              result.created(),
              result.alreadyPresent(),
              result.failures());
        }
        row.put(statusKey, status);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
        return status;
      }
    } catch (Exception e) {
      log.error("{}: partition {} ({}) failed", id(), partition.id(), partition.label(), e);
      synchronized (row) {
        row.remove(cursorKey);
        row.put(statusKey, PARTITION_FAILED);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
      }
      return PARTITION_FAILED;
    }
  }

  /**
   * What an attempt at the partition resumes from, if anything. A paused partition hands over its
   * checkpoint and stops being paused on the row, there and then, so a cursor the attempt persists
   * on its way is never resumed from should the run die: only another actual pause leaves one.
   */
  @Nullable
  static CrossClusterCopier.Checkpoint claimPartition(
      @Nonnull final ClusterMigrationRow row, @Nonnull final CopyPartition partition) {
    final String cursorKey = ClusterMigrationState.partitionKey(partition.id(), PARTITION_CURSOR);
    final String statusKey = ClusterMigrationState.partitionKey(partition.id(), PARTITION_STATUS);
    synchronized (row) {
      final boolean paused = PARTITION_PAUSED.equals(row.get(statusKey));
      final CrossClusterCopier.Checkpoint resumeFrom = paused ? checkpointOf(row, partition) : null;
      row.remove(cursorKey);
      if (paused) {
        row.remove(statusKey);
        row.persist(DataHubUpgradeState.IN_PROGRESS);
      }
      return resumeFrom;
    }
  }

  @Nonnull
  private static CrossClusterCopier.Checkpoint checkpointOf(
      @Nonnull final ClusterMigrationRow row, @Nonnull final CopyPartition partition) {
    return new CrossClusterCopier.Checkpoint(
        row.get(ClusterMigrationState.partitionKey(partition.id(), PARTITION_CURSOR)),
        count(row, partition, PARTITION_READ),
        count(row, partition, PARTITION_CREATED),
        count(row, partition, PARTITION_PRESENT),
        count(row, partition, PARTITION_FAILURES));
  }

  private static long count(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final CopyPartition partition,
      @Nonnull final String property) {
    return row.getLong(ClusterMigrationState.partitionKey(partition.id(), property)).orElse(0L);
  }

  /**
   * A transient failure (a connection that timed out, a page that never arrived) is tried again
   * after a growing pause instead of failing the family's run. The first attempt may resume a
   * paused partition; every retry starts the partition over, since the failed attempt's cursor
   * cannot be trusted.
   */
  @Nonnull
  static CrossClusterCopier.Result copyWithRetries(
      @Nonnull final CopyPartition partition,
      @Nullable final CrossClusterCopier.Checkpoint resumeFrom,
      @Nonnull final CrossClusterCopier copier,
      @Nonnull final CrossClusterCopier.Checkpointer checkpointer,
      @Nonnull final BooleanSupplier shouldStop,
      final long backoffMs)
      throws IOException, InterruptedException {
    IOException last = null;
    for (int attempt = 1; attempt <= PARTITION_ATTEMPTS; attempt++) {
      try {
        return copier.copy(partition, attempt == 1 ? resumeFrom : null, checkpointer, shouldStop);
      } catch (IOException e) {
        last = e;
        if (attempt == PARTITION_ATTEMPTS) {
          break;
        }
        final long pause = backoffMs * (1L << (attempt - 1));
        log.warn(
            "Copy of {} failed on attempt {} of {}: {}; starting it over in {} ms",
            partition.label(),
            attempt,
            PARTITION_ATTEMPTS,
            e.toString(),
            pause);
        Thread.sleep(pause);
      }
    }
    throw last;
  }

  private static void putCounts(
      @Nonnull final ClusterMigrationRow row,
      final int partitionId,
      final long read,
      final long created,
      final long present,
      final long failures) {
    row.put(ClusterMigrationState.partitionKey(partitionId, PARTITION_READ), read);
    row.put(ClusterMigrationState.partitionKey(partitionId, PARTITION_CREATED), created);
    row.put(ClusterMigrationState.partitionKey(partitionId, PARTITION_PRESENT), present);
    row.put(ClusterMigrationState.partitionKey(partitionId, PARTITION_FAILURES), failures);
  }

  // ---- window pass ---------------------------------------------------------------------------

  /**
   * Drops the state of an earlier windowed pass so the next one runs in full. Its partitions keep
   * the same ids every time, and {@link #runPartitions} skips what the row says is done.
   */
  static void forgetWindowPartitions(@Nonnull final ClusterMigrationRow row) {
    row.state()
        .keySet()
        .removeIf(
            key -> {
              if (!key.startsWith(ClusterMigrationState.PARTITION_PREFIX)) {
                return false;
              }
              final String rest = key.substring(ClusterMigrationState.PARTITION_PREFIX.length());
              final int dot = rest.indexOf('.');
              return dot > 0 && Integer.parseInt(rest.substring(0, dot)) >= WINDOW_PARTITION_BASE;
            });
  }

  /** How many documents the last windowed pass wrote on the target, over all of its partitions. */
  static long writtenByWindowPass(@Nonnull final ClusterMigrationRow row) {
    long written = 0;
    for (Map.Entry<String, String> entry : row.state().entrySet()) {
      if (!entry.getKey().startsWith(ClusterMigrationState.PARTITION_PREFIX)) {
        continue;
      }
      final String rest = entry.getKey().substring(ClusterMigrationState.PARTITION_PREFIX.length());
      final int dot = rest.indexOf('.');
      if (dot <= 0
          || Integer.parseInt(rest.substring(0, dot)) < WINDOW_PARTITION_BASE
          || !PARTITION_CREATED.equals(rest.substring(dot + 1))) {
        continue;
      }
      written += Long.parseLong(entry.getValue());
    }
    return written;
  }

  /**
   * The copier of the windowed pass. A mutable family's pass replaces documents the target holds in
   * an older version, which only the in-process copier can do (a remote reindex never sees the
   * target's document), so that pass runs in process whatever the copy mode.
   */
  @Nonnull
  private CrossClusterCopier windowCopier(
      @Nonnull final CopyMode mode,
      @Nonnull final CrossClusterCopier copier,
      @Nonnull final CopySettings copy,
      @Nonnull final SearchClusterRegistry.ClusterConnection source,
      @Nonnull final SearchClusterRegistry.ClusterConnection target,
      @Nonnull final String upgradeId) {
    if (!family.isMutable() || mode != CopyMode.REMOTE_REINDEX) {
      return copier;
    }
    return CopierFactory.create(opContext, CopyMode.IN_PROCESS, copy, source, target, upgradeId);
  }

  /**
   * Insurance over everything written since the live writers started mirroring (or since the
   * operator's snapshot): a pass filtered on the family's time field. Usage events, written once,
   * copy with create semantics, which restores anything the mirror lost; every other family's
   * documents are updated in place, so the pass replaces each document in the window unless the
   * target's is current, and a lost update is repaired without undoing a later mirrored write.
   * Rollover layouts are covered by the mirror into the live staging index and by the post-flip
   * sweep, so only concrete indices get a pass.
   */
  @Nonnull
  private PartitionOutcome runWindowPass(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final List<SourceIndexSet> sourceSets,
      @Nonnull final CrossClusterCopier copier,
      final long windowStart,
      @Nonnull final BooleanSupplier shouldStop)
      throws InterruptedException {
    final List<CopyPartition> partitions = new ArrayList<>();
    final String guardField = family.isMutable() ? family.timeField() : null;
    int i = 0;
    for (SourceIndexSet set : sourceSets) {
      if (set.isRollover()) {
        continue;
      }
      final ObjectNode filter = family.windowFilter(opContext.getObjectMapper(), windowStart, null);
      partitions.add(
          new CopyPartition(
              WINDOW_PARTITION_BASE + i++,
              set.logicalName(),
              migration.getNames().target(opContext, set.logicalName()),
              "window " + set.logicalName() + " " + family.timeField() + ">=" + windowStart,
              filter,
              -1,
              guardField));
    }
    if (partitions.isEmpty()) {
      return new PartitionOutcome(0, 0, 0);
    }
    return runPartitions(row, partitions, copier, 1, shouldStop);
  }

  // ---- adoption ------------------------------------------------------------------------------

  /**
   * Usage only: every staging index on the target joins its rollover alias or data stream, and the
   * sweep that finishes the live staging index after the flip is owed from here on.
   */
  private void adoptStagingIndices(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final ClusterHttp targetHttp,
      @Nullable final String liveStaging)
      throws IOException {
    if (liveStaging == null) {
      return;
    }
    new UsageStagingAdoption(opContext).adopt(targetHttp, migration, liveStaging);
    row.put(ClusterMigrationState.USAGE_POST_FLIP_SWEEP, "pending");
  }
}
