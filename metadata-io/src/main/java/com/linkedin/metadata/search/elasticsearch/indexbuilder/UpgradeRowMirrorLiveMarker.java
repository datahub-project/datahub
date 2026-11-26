package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.update.MirroredWriteLog;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This process's periodic look at a family's migration row. Each {@link #poll} records {@code
 * mirrorActiveSince.<process>} for every role this process plays (GMS, the MAE consumer, system
 * update; a GMS running the embedded MAE consumer is both live writers at once) when the row's
 * marker is missing or belongs to an earlier rollout, and keeps the mirror's delete log in step
 * with the copy: the log stops once the row reads verified or the prepare step set the copy aside,
 * since nothing will replay it, and starts again should the copy be planned anew.
 *
 * <p>The marker is this process's start time: from the moment a process starts with the mirror
 * configured, every write it makes to the family is mirrored. The prepare step stamps the row with
 * each rollout's start, before the rollout's pods exist, so a marker older than the stamp was
 * recorded by a pod of an earlier rollout and is recorded over, while the first pod of this rollout
 * to record a role keeps it, as the earliest moment the rollout was mirroring. Polling starts at
 * startup; a process that starts before the row exists (a developer stack, or a pod racing the
 * upgrade) simply finds it on a later poll. Persistence failures are logged and left to the next
 * poll: the live writes never depend on this bookkeeping.
 */
@Slf4j
public final class UpgradeRowMirrorLiveMarker implements UpgradeRowPoll {
  static final long POLL_INTERVAL_MS = 60_000L;

  private final DataHubUpgradeResultStore store;
  private final OperationContext systemOpContext;
  private final Urn upgradeUrn;
  private final List<String> keys;
  @Nullable private final MirroredWriteLog writeLog;
  private final long startedAt = System.currentTimeMillis();
  private volatile boolean recorded;
  private volatile boolean rowMissingReported;
  private volatile boolean startedBeforeRolloutReported;

  /**
   * @param writeLog the mirror's delete log to switch off once the copy has verified; null for a
   *     mirror that keeps no log
   */
  public UpgradeRowMirrorLiveMarker(
      @Nonnull final DataHubUpgradeResultStore store,
      @Nonnull final OperationContext systemOpContext,
      @Nonnull final Urn upgradeUrn,
      @Nonnull final Set<ClusterMigrationState.Process> processes,
      @Nullable final MirroredWriteLog writeLog) {
    if (processes.isEmpty()) {
      throw new IllegalArgumentException("A mirror-live marker needs at least one process role");
    }
    this.store = store;
    this.systemOpContext = systemOpContext;
    this.upgradeUrn = upgradeUrn;
    this.keys = processes.stream().map(ClusterMigrationState::mirrorActiveSinceKey).toList();
    this.writeLog = writeLog;
  }

  /** Whether the last poll found every marker of this process on the row, for this rollout. */
  public boolean isRecorded() {
    return recorded;
  }

  @Override
  @Nonnull
  public Urn getUpgradeUrn() {
    return upgradeUrn;
  }

  /** Reads the row once: records the markers it lacks and sets the delete log from its status. */
  @Override
  public void poll() {
    try {
      final EnvelopedAspect latest = store.readLatest(systemOpContext, upgradeUrn);
      final DataHubUpgradeResult prior =
          DataHubUpgradeResultConditionalPersist.fromEnveloped(latest);
      if (prior == null || prior.getResult() == null) {
        rowMissing();
        return;
      }
      rowMissingReported = false;
      final Optional<ClusterMigrationState.Status> status =
          ClusterMigrationState.getStatus(prior.getResult());
      if (writeLog != null) {
        // A copy replays the entries; none will once the row is verified or the prepare step set
        // the copy aside.
        final boolean copyMayStillRun =
            status.map(s -> !s.isTerminal()).orElse(true)
                && !"false".equals(prior.getResult().get(ClusterMigrationState.COPY_ENABLED));
        writeLog.setActive(copyMayStillRun);
      }
      final List<String> stale =
          keys.stream()
              .filter(key -> !ClusterMigrationState.marksCurrentRollout(prior.getResult(), key))
              .toList();
      if (stale.isEmpty()) {
        recorded = true;
        return;
      }
      if (startedAt
          < ClusterMigrationState.getLong(
                  prior.getResult(), ClusterMigrationState.ROLLOUT_STARTED_AT)
              .orElse(Long.MIN_VALUE)) {
        // This process predates the rollout the row was stamped for (the system-update job that
        // stamped it, a pod up since before the blocking job): a pod of that rollout records the
        // markers; a marker from here would only say it is stale.
        startedBeforeRollout();
        return;
      }
      final String mine = String.valueOf(startedAt);
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          systemOpContext,
          store,
          upgradeUrn,
          (map, existingState) -> {
            for (String key : stale) {
              // Another pod of this rollout may have got there first; its start is the earlier
              // moment the rollout was mirroring, so it stays.
              if (!ClusterMigrationState.marksCurrentRollout(map, key)) {
                map.put(key, mine);
              }
            }
            return existingState != null ? existingState : prior.getState();
          },
          DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);
      recorded = true;
      log.info("Recorded mirror-live markers {} = {} on {}", stale, mine, upgradeUrn);
    } catch (Exception e) {
      log.warn(
          "Could not record mirror-live markers {} on {}; trying again in {} ms: {}",
          keys,
          upgradeUrn,
          POLL_INTERVAL_MS,
          e.toString());
    }
  }

  private void startedBeforeRollout() {
    recorded = false;
    if (!startedBeforeRolloutReported) {
      startedBeforeRolloutReported = true;
      log.info(
          "This process started before the rollout {} was last prepared for; its mirror-live"
              + " markers {} are the newer pods' to record",
          upgradeUrn,
          keys);
    }
  }

  private void rowMissing() {
    recorded = false;
    if (writeLog != null) {
      writeLog.setActive(true);
    }
    if (!rowMissingReported) {
      rowMissingReported = true;
      log.info(
          "Migration row {} does not exist yet; the mirror-live markers {} wait for the blocking"
              + " upgrade step",
          upgradeUrn,
          keys);
    }
  }
}
