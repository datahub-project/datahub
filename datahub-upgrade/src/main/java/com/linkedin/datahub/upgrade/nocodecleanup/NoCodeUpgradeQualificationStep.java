package com.linkedin.datahub.upgrade.nocodecleanup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.entity.AspectStorageValidationUtil;
import io.ebean.EbeanServer;
import java.util.function.Function;


public class NoCodeUpgradeQualificationStep implements UpgradeStep<Void> {

  private final EbeanServer _server;

  NoCodeUpgradeQualificationStep(EbeanServer server) {
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
        if (!AspectStorageValidationUtil.checkV2TableExists(_server)) {
          // Unqualified (V2 Table does not exist)
          return new DefaultUpgradeStepResult<>(
              id(),
              UpgradeStepResult.Result.SUCCEEDED,
              UpgradeStepResult.Action.ABORT,
              "You have not successfully migrated yet. Aborting the cleanup...");
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
