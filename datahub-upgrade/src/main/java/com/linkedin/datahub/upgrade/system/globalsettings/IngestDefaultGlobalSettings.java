package com.linkedin.datahub.upgrade.system.globalsettings;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import javax.annotation.Nonnull;

public class IngestDefaultGlobalSettings implements BlockingSystemUpgrade {

  private final List<UpgradeStep> stepList;

  public IngestDefaultGlobalSettings(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    stepList = ImmutableList.of(new IngestDefaultGlobalSettingsUpgradeStep(entityService, enabled));
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
