package com.linkedin.datahub.upgrade.system.vianodes;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * A job that reindexes all datajob inputoutput aspects as part of the via node upgrade. This is
 * required to index column-level lineage correctly using via nodes.
 */
@Slf4j
public class ReindexDataJobViaNodesCLL implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public ReindexDataJobViaNodesCLL(
      EntityService<?> entityService, boolean enabled, Integer batchSize) {
    if (enabled) {
      _steps = ImmutableList.of(new ReindexDataJobViaNodesCLLStep(entityService, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return this.getClass().getName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
