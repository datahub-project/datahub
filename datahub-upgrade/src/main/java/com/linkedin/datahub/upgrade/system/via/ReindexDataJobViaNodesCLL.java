package com.linkedin.datahub.upgrade.system.via;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * A job that reindexes all datajob inputoutput aspects as part of the via node upgrade. This is
 * required to index column-level lineage correctly using via nodes.
 */
@Slf4j
public class ReindexDataJobViaNodesCLL implements Upgrade {

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
