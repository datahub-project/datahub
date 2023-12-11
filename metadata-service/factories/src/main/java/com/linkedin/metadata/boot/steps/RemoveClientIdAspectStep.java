package com.linkedin.metadata.boot.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import java.util.HashMap;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveClientIdAspectStep implements BootstrapStep {

  private final EntityService _entityService;

  private static final String UPGRADE_ID = "remove-unknown-aspects";
  private static final String INVALID_TELEMETRY_ASPECT_NAME = "clientId";
  private static final Urn REMOVE_UNKNOWN_ASPECTS_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute() throws Exception {
    try {
      if (_entityService.exists(REMOVE_UNKNOWN_ASPECTS_URN)) {
        log.info("Unknown aspects have been removed. Skipping...");
        return;
      }
      // Remove invalid telemetry aspect
      _entityService.deleteAspect(
          TelemetryUtils.CLIENT_ID_URN, INVALID_TELEMETRY_ASPECT_NAME, new HashMap<>(), true);

      BootstrapStep.setUpgradeResult(REMOVE_UNKNOWN_ASPECTS_URN, _entityService);
    } catch (Exception e) {
      log.error("Error when running the RemoveUnknownAspects Bootstrap Step", e);
      _entityService.deleteUrn(REMOVE_UNKNOWN_ASPECTS_URN);
      throw new RuntimeException("Error when running the RemoveUnknownAspects Bootstrap Step", e);
    }
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }
}
