package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.AspectStorageValidationUtil;
import io.ebean.EbeanServer;
import java.util.function.Function;


public class UpgradeQualificationStep implements UpgradeStep<Void> {

  private final EbeanServer _server;

  UpgradeQualificationStep(EbeanServer server) {
    _server = server;
  }

  @Override
  public String id() {
    return "UpgradeQualificationStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult<Void>> executable() {
    return (context) -> {
      try {
        if (AspectStorageValidationUtil.checkV2TableExists(_server)) {
          // Unqualified (Table already exists)
          return new DefaultUpgradeStepResult<>(
              id(),
              UpgradeStepResult.Result.SUCCEEDED,
              UpgradeStepResult.Action.ABORT,
              "Failed to qualify upgrade candidate. Aborting the upgrade...");
        } else {
          // Qualified.
          return new DefaultUpgradeStepResult<>(
              id(),
              UpgradeStepResult.Result.SUCCEEDED,
              "Found qualified upgrade candidate. Proceeding with upgrade...");
        }
      } catch (Exception e) {
        return new DefaultUpgradeStepResult<>(
            id(),
            UpgradeStepResult.Result.FAILED,
            String.format("Failed to check if metadata_aspect_v2 table exists: %s", e.toString()));
      }
    };
  }
}
