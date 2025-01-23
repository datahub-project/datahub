package com.linkedin.datahub.upgrade.removeunknownaspects;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.HashMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveClientIdAspectStep implements UpgradeStep {

  private static final String INVALID_CLIENT_ID_ASPECT = "clientId";

  private final EntityService<?> _entityService;

  @Override
  public String id() {
    return this.getClass().getSimpleName();
  }

  @Override
  public boolean skip(UpgradeContext context) {
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return upgradeContext -> {
      _entityService.deleteAspect(
          upgradeContext.opContext(),
          TelemetryUtils.CLIENT_ID_URN,
          INVALID_CLIENT_ID_ASPECT,
          new HashMap<>(),
          true);
      return (UpgradeStepResult) new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
