package com.linkedin.datahub.upgrade.system.dataproducts;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

/**
 * Non-blocking system upgrade that materializes asset-side {@code dataProducts} from stored {@code
 * dataProductProperties} via {@link ResyncDataProductAssetsStep}.
 *
 * <p>Runs by default on first upgrade (gated by {@code systemUpdate.dataProductAssets.enabled}).
 * Subsequent runs skip once the versioned upgrade marker is {@code SUCCEEDED}, unless {@code
 * systemUpdate.dataProductAssets.reprocess.enabled} is true.
 */
public class ResyncDataProductAssets implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public ResyncDataProductAssets(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit,
      boolean reprocessEnabled) {
    if (enabled) {
      steps =
          ImmutableList.of(
              new ResyncDataProductAssetsStep(
                  opContext,
                  entityService,
                  aspectDao,
                  batchSize,
                  batchDelayMs,
                  limit,
                  reprocessEnabled));
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
