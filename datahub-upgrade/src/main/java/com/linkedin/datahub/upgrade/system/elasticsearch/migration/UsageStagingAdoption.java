package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Makes every usage staging index on the target a historical member of its rollover alias or data
 * stream once the copy is complete. The staging indices are found by name on the target rather than
 * taken from the plan, so an operator's restore (pathway C, which plans nothing) and an adoption
 * interrupted halfway are covered alike: a member already in place is only settled again, which is
 * idempotent. The live staging index still receives mirrored writes until the flip, so it joins as
 * a member that nothing finishes yet; the sweep after the flip does.
 */
@Slf4j
final class UsageStagingAdoption {
  private final OperationContext opContext;

  UsageStagingAdoption(@Nonnull final OperationContext opContext) {
    this.opContext = opContext;
  }

  void adopt(
      @Nonnull final ClusterHttp target,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final String liveStaging)
      throws IOException {
    final String token = migration.getNames().targetToken(opContext);
    final String targetLogical = token + Constants.DATAHUB_USAGE_EVENT_INDEX;
    final SourceIndexSet targetSet =
        IndexLayoutResolver.resolve(target, SearchComponent.USAGE, targetLogical);
    if (targetSet == null || !targetSet.isRollover()) {
      throw new IllegalStateException(
          targetLogical
              + " on cluster '"
              + migration.getTargetCluster()
              + "' is not a rollover alias or data stream; the prepare step creates one");
    }
    final StagingIndexAdopter adopter = new StagingIndexAdopter(target);
    int adopted = 0;
    for (String staging :
        target.resolveIndices(token + MigrationIndexNames.USAGE_STAGING_BASE + "-*")) {
      if (staging.equals(liveStaging)) {
        continue;
      }
      if (targetSet.backingIndices().contains(staging)) {
        adopter.settle(targetSet.layout(), staging, true);
        continue;
      }
      adopter.adopt(targetSet.layout(), targetLogical, staging, true);
      adopted++;
    }
    if (targetSet.backingIndices().contains(liveStaging)) {
      adopter.settle(targetSet.layout(), liveStaging, false);
    } else {
      adopter.adopt(targetSet.layout(), targetLogical, liveStaging, false);
    }
    log.info(
        "Adopted {} staging index(es) and the live staging index {} into {}",
        adopted,
        liveStaging,
        targetLogical);
  }
}
