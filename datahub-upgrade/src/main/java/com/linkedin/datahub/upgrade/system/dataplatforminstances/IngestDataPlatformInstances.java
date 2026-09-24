package com.linkedin.datahub.upgrade.system.dataplatforminstances;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Non-blocking system upgrade that backfills DataPlatformInstance aspects for all existing
 * entities. Skips if the aspect already exists in the database (i.e. migration already ran).
 */
public class IngestDataPlatformInstances implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> stepList;

  public IngestDataPlatformInstances(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final AspectMigrationsDao migrationsDao,
      final boolean enabled) {
    stepList =
        ImmutableList.of(
            new IngestDataPlatformInstancesUpgradeStep(entityService, migrationsDao, enabled));
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return stepList;
  }
}
