package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.AspectStorageValidationUtil;
import io.ebean.EbeanServer;
import java.util.function.Function;

public class UpgradeQualificationStep implements UpgradeStep {

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
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      if (context.parsedArgs().containsKey(NoCodeUpgrade.FORCE_UPGRADE_ARG_NAME)) {
        context.report().addLine("Forced upgrade detected. Proceeding with upgrade...");
        return new DefaultUpgradeStepResult(
            id(),
            UpgradeStepResult.Result.SUCCEEDED);
      }

      try {
        if (isQualified(_server)) {
          // Qualified.
          context.report().addLine("Found qualified upgrade candidate. Proceeding with upgrade...");
          return new DefaultUpgradeStepResult(
              id(),
              UpgradeStepResult.Result.SUCCEEDED);
        }
        // Unqualified (Table already exists)
        context.report().addLine("Failed to qualify upgrade candidate. Aborting the upgrade...");
        return new DefaultUpgradeStepResult(
            id(),
            UpgradeStepResult.Result.SUCCEEDED,
            UpgradeStepResult.Action.ABORT);
      } catch (Exception e) {
        context.report().addLine(String.format("Failed to check if metadata_aspect_v2 table exists: %s", e.toString()));
        return new DefaultUpgradeStepResult(
            id(),
            UpgradeStepResult.Result.FAILED);
      }
    };
  }

  private boolean isQualified(EbeanServer server) {

    return AspectStorageValidationUtil.checkV1TableExists(server)
        && (
            !AspectStorageValidationUtil.checkV2TableExists(server)
                || AspectStorageValidationUtil.getV2RowCount(server) == 0
           );
  }
}
