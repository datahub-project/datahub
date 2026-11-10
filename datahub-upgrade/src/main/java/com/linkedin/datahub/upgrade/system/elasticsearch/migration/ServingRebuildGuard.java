package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Whether this deploy's Phase 1 left a serving index of the family mid-rebuild. A copy that reads
 * across an alias swap on the source would straddle two snapshots, so the migration waits.
 */
@Slf4j
public final class ServingRebuildGuard {
  private ServingRebuildGuard() {}

  public static boolean inFlight(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final String upgradeVersion,
      @Nonnull final List<String> logicalNames) {
    final Optional<DataHubUpgradeResult> phase1 =
        BootstrapStep.getUpgradeResult(
            opContext,
            BootstrapStep.getUpgradeUrn(
                IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + upgradeVersion),
            entityService);
    if (phase1.isEmpty() || phase1.get().getResult() == null) {
      return false;
    }
    final Map<String, String> phase1Result = phase1.get().getResult();
    for (String name : logicalNames) {
      final Optional<IncrementalReindexState.Status> status =
          IncrementalReindexState.getStatus(phase1Result, name);
      if (status.isPresent()
          && status.get() != IncrementalReindexState.Status.COMPLETED
          && status.get() != IncrementalReindexState.Status.DUAL_WRITE_DISABLED) {
        log.warn("Serving index {} is {} in Phase 1 of this deploy", name, status.get());
        return true;
      }
    }
    return false;
  }
}
