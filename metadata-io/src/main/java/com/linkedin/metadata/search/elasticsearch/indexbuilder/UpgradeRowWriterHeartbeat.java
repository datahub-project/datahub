package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * This process's minute report that it is a live writer: {@code writer.<instance>} on the shared
 * writers row, carrying the process's start time and the time of the report. The copy step waits
 * until no writer that started before the rollout's stamp is still reporting, which is how it knows
 * that the previous rollout's pods, which do not mirror, have stopped writing. Every GMS and MAE
 * consumer reports while a second cluster is declared, whether or not it migrates anything: the
 * pods whose absence matters are exactly those whose configuration knows no migration. Reports
 * older than {@link ClusterMigrationState#WRITER_RETENTION_MS} are dropped on the way, so pods that
 * come and go do not grow the row without bound. A failed report is logged and left to the next
 * poll; the live writes never depend on this bookkeeping.
 */
@Slf4j
public final class UpgradeRowWriterHeartbeat implements UpgradeRowPoll {
  private final DataHubUpgradeResultStore store;
  private final OperationContext systemOpContext;
  private final Urn upgradeUrn;
  private final String instance;
  private final long startedAt = System.currentTimeMillis();

  public UpgradeRowWriterHeartbeat(
      @Nonnull final DataHubUpgradeResultStore store,
      @Nonnull final OperationContext systemOpContext,
      @Nonnull final Urn upgradeUrn,
      @Nonnull final String instance) {
    this.store = store;
    this.systemOpContext = systemOpContext;
    this.upgradeUrn = upgradeUrn;
    this.instance = instance;
  }

  /** This host's name (the pod's, on Kubernetes), or a random id when it has none. */
  @Nonnull
  public static String localInstance() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return UUID.randomUUID().toString();
    }
  }

  @Nonnull
  public String getInstance() {
    return instance;
  }

  @Override
  @Nonnull
  public Urn getUpgradeUrn() {
    return upgradeUrn;
  }

  @Override
  public void poll() {
    final long now = System.currentTimeMillis();
    try {
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          systemOpContext,
          store,
          upgradeUrn,
          (map, existingState) -> {
            map.entrySet()
                .removeIf(
                    entry -> {
                      final ClusterMigrationState.Writer writer =
                          ClusterMigrationState.parseWriter(entry.getKey(), entry.getValue());
                      return writer != null
                          && writer.lastSeen() < now - ClusterMigrationState.WRITER_RETENTION_MS;
                    });
            map.put(
                ClusterMigrationState.writerKey(instance),
                ClusterMigrationState.writerValue(startedAt, now));
            // The row is a register, not an upgrade run; its state only has to exist.
            return existingState != null ? existingState : DataHubUpgradeState.SUCCEEDED;
          },
          DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);
    } catch (Exception e) {
      log.warn(
          "Could not report writer {} on {}; trying again in {} ms: {}",
          instance,
          upgradeUrn,
          UpgradeRowMirrorLiveMarker.POLL_INTERVAL_MS,
          e.toString());
    }
  }
}
