package com.linkedin.datahub.upgrade.system.dataproducts;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

/**
 * Optional reprocess escape hatch that re-upserts {@code dataProductProperties} so {@code
 * DataProductAssetsSideEffect} re-syncs asset-side {@code dataProducts} membership.
 *
 * <p>First-time population is driven by {@code dataProductProperties} schemaVersion + {@code
 * MigrateAspects} / ZDU. This upgrade only runs when {@code
 * systemUpdate.dataProductAssets.reprocess.enabled=true}.
 */
public class ResyncDataProductAssets implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public ResyncDataProductAssets(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (reprocessEnabled) {
      steps =
          ImmutableList.of(
              new ResyncDataProductAssetsStep(
                  opContext, entityService, searchService, true, batchSize));
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "ResyncDataProductAssets";
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
