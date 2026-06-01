package com.linkedin.datahub.upgrade.system.telemetry;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.HashMap;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoveClientIdAspectStep implements UpgradeStep {

  private static final String UPGRADE_ID = "remove-unknown-aspects";
  private static final String INVALID_TELEMETRY_ASPECT_NAME = "clientId";
  private static final Urn REMOVE_UNKNOWN_ASPECTS_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final EntityService<?> _entityService;
  private final boolean _enabled;

  public RemoveClientIdAspectStep(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    _entityService = entityService;
    _enabled = enabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!_enabled) {
      log.info("RemoveClientIdAspect is disabled. Skipping.");
      return true;
    }
    boolean skip = _entityService.exists(context.opContext(), REMOVE_UNKNOWN_ASPECTS_URN, true);
    if (skip) {
      log.info("Unknown aspects have been removed. Skipping...");
    }
    return skip;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        _entityService.deleteAspect(
            context.opContext(),
            TelemetryUtils.CLIENT_ID_URN,
            INVALID_TELEMETRY_ASPECT_NAME,
            new HashMap<>(),
            true);
        BootstrapStep.setUpgradeResult(
            context.opContext(), REMOVE_UNKNOWN_ASPECTS_URN, _entityService);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.warn("Error when running the RemoveUnknownAspects Bootstrap Step", e);
        _entityService.deleteUrn(context.opContext(), REMOVE_UNKNOWN_ASPECTS_URN);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
