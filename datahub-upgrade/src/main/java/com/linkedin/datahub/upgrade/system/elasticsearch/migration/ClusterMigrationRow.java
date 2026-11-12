package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One family migration's persisted state: the flat result map of the {@code dataHubUpgradeResult}
 * aspect under {@code ClusterMigration_<family>_<source>_to_<target>}. Loaded once per step and
 * written back whole under optimistic locking, like the incremental reindex rows.
 */
public final class ClusterMigrationRow {
  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final Urn urn;
  private final Map<String, String> state;
  private final boolean existed;
  @Nullable private DataHubUpgradeState upgradeState;

  private ClusterMigrationRow(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final Urn urn,
      @Nonnull final Map<String, String> state,
      final boolean existed,
      @Nullable final DataHubUpgradeState upgradeState) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.urn = urn;
    this.state = state;
    this.existed = existed;
    this.upgradeState = upgradeState;
  }

  @Nonnull
  public static ClusterMigrationRow load(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final String upgradeId) {
    final Urn urn = BootstrapStep.getUpgradeUrn(upgradeId);
    final Optional<DataHubUpgradeResult> result =
        BootstrapStep.getUpgradeResult(opContext, urn, entityService);
    final Map<String, String> state = new HashMap<>();
    DataHubUpgradeState upgradeState = null;
    if (result.isPresent()) {
      if (result.get().getResult() != null) {
        state.putAll(result.get().getResult());
      }
      upgradeState = result.get().getState();
    }
    return new ClusterMigrationRow(
        opContext, entityService, urn, state, result.isPresent(), upgradeState);
  }

  @Nonnull
  public Urn getUrn() {
    return urn;
  }

  public boolean existed() {
    return existed;
  }

  @Nonnull
  public Map<String, String> state() {
    return state;
  }

  @Nullable
  public String get(@Nonnull final String key) {
    return state.get(key);
  }

  @Nonnull
  public Optional<Long> getLong(@Nonnull final String key) {
    return ClusterMigrationState.getLong(state, key);
  }

  public void put(@Nonnull final String key, @Nonnull final String value) {
    state.put(key, value);
  }

  public void put(@Nonnull final String key, final long value) {
    state.put(key, String.valueOf(value));
  }

  public void putIfAbsent(@Nonnull final String key, @Nonnull final String value) {
    state.putIfAbsent(key, value);
  }

  public void remove(@Nonnull final String key) {
    state.remove(key);
  }

  @Nonnull
  public Optional<ClusterMigrationState.Status> status() {
    return ClusterMigrationState.getStatus(state);
  }

  public void setStatus(@Nonnull final ClusterMigrationState.Status status) {
    state.put(ClusterMigrationState.STATUS, status.name());
  }

  /** Clears everything so a migration can be planned again from nothing. */
  public void reset() {
    state.clear();
  }

  /**
   * Writes the row back whole, except for the mirror markers: the live writers' polls record them
   * on their own clock, and a marker that landed since this row was loaded, or that is newer than
   * the one loaded, is kept rather than overwritten by the snapshot a step worked from.
   */
  public void persist(@Nonnull final DataHubUpgradeState newState) {
    try {
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          opContext,
          entityService,
          urn,
          (stored, existingState) -> {
            final Map<String, String> recordedMeanwhile = new HashMap<>();
            stored.forEach(
                (key, value) -> {
                  if (key.startsWith(ClusterMigrationState.MIRROR_ACTIVE_SINCE_PREFIX)
                      && isNewerMarker(value, state.get(key))) {
                    recordedMeanwhile.put(key, value);
                  }
                });
            state.putAll(recordedMeanwhile);
            stored.clear();
            stored.putAll(state);
            return newState;
          });
      upgradeState = newState;
    } catch (Exception e) {
      throw new RuntimeException("Could not persist migration row " + urn, e);
    }
  }

  private static boolean isNewerMarker(@Nonnull final String stored, @Nullable final String mine) {
    return mine == null || Long.parseLong(stored) > Long.parseLong(mine);
  }

  @Nullable
  public DataHubUpgradeState upgradeState() {
    return upgradeState;
  }
}
