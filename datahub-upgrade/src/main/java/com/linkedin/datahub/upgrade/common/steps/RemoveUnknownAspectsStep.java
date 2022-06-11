package com.linkedin.datahub.upgrade.common.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.entity.EntityService;
import java.util.HashMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class RemoveUnknownAspectsStep implements UpgradeStep {

  private static final String INVALID_CLIENT_ID_ASPECT = "clientId";

  private final EntityService _entityService;

  @Override
  public String id() {
    return this.getClass().getSimpleName();
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (context.parsedArgs().containsKey(NoCodeUpgrade.REMOVE_UNKNOWN_ARG_NAME)) {
      return false;
    }
    return true;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    _entityService.deleteAspect(TelemetryUtils.CLIENT_ID_URN, INVALID_CLIENT_ID_ASPECT, new HashMap<>(), true);
    return upgradeContext -> {
      _entityService.deleteAspect(TelemetryUtils.CLIENT_ID_URN, INVALID_CLIENT_ID_ASPECT,
          new HashMap<>(), true);
      return (UpgradeStepResult) new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
