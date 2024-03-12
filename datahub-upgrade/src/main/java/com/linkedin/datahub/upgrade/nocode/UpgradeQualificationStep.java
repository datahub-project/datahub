package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.entity.ebean.AspectStorageValidationUtil;
import io.ebean.Database;
import java.util.function.Function;

public class UpgradeQualificationStep implements UpgradeStep {

  private final Database _server;

  UpgradeQualificationStep(Database server) {
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
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
      }

      try {
        if (isQualified(_server, context)) {
          // Qualified.
          context.report().addLine("Found qualified upgrade candidate. Proceeding with upgrade...");
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
        }
        // Unqualified (Table already exists)
        context.report().addLine("Failed to qualify upgrade candidate. Aborting the upgrade...");
        return new DefaultUpgradeStepResult(
            id(), UpgradeStepResult.Result.SUCCEEDED, UpgradeStepResult.Action.ABORT);
      } catch (Exception e) {
        context.report().addLine("Failed to check if metadata_aspect_v2 table exists", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
    };
  }

  // Check whether the upgrade is needed
  private boolean isQualified(Database server, UpgradeContext context) {
    boolean v1TableExists = AspectStorageValidationUtil.checkV1TableExists(server);
    if (v1TableExists) {
      context.report().addLine("-- V1 table exists");
      long v1TableRowCount = AspectStorageValidationUtil.getV1RowCount(server);
      context.report().addLine(String.format("-- V1 table has %d rows", v1TableRowCount));
      boolean v2TableExists = AspectStorageValidationUtil.checkV2TableExists(server);
      if (v2TableExists) {
        context.report().addLine("-- V2 table exists");
        long v2TableRowCount = AspectStorageValidationUtil.getV2NonSystemRowCount(server);
        if (v2TableRowCount == 0) {
          context.report().addLine("-- V2 table is empty");
          return true;
        }
        context.report().addLine(String.format("-- V2 table has %d rows", v2TableRowCount));
        context
            .report()
            .addLine("-- Since V2 table has records, we will not proceed with the upgrade. ");
        context
            .report()
            .addLine(
                "-- If V2 table has significantly less rows, consider running the forced upgrade. ");
        return false;
      }
      context.report().addLine("-- V2 table does not exist");
      return true;
    }
    context.report().addLine("-- V1 table does not exist");
    return false;
  }
}
