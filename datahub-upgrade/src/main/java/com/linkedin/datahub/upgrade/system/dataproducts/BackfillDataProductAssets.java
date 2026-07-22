package com.linkedin.datahub.upgrade.system.dataproducts;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

/**
 * Backfills the asset-side {@code dataProducts} aspect from existing Data Product membership. Gated
 * by {@code systemUpdate.dataProductAssets.enabled} (bean creation) and the {@code
 * BACKFILL_DATA_PRODUCT_ASSETS} env var (actual execution — see the step's skip()).
 */
public class BackfillDataProductAssets implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public BackfillDataProductAssets(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      Integer batchSize) {
    if (enabled) {
      steps =
          ImmutableList.of(
              new BackfillDataProductAssetsStep(
                  opContext, entityService, searchService, batchSize));
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillDataProductAssets";
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
