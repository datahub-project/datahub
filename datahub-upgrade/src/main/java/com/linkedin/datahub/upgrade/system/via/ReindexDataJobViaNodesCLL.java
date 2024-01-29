package com.linkedin.datahub.upgrade.system.via;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReindexDataJobViaNodesCLL implements Upgrade {

  private final List<UpgradeStep> _steps;

  public ReindexDataJobViaNodesCLL(EntityService<?> entityService) {
    _steps = ImmutableList.of(new ReindexDataJobViaNodesCLLStep(entityService));
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
