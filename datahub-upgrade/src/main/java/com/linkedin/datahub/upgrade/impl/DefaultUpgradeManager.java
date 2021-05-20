package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeManager;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DefaultUpgradeManager implements UpgradeManager {

  private final Map<String, Upgrade> _upgrades = new HashMap<>();

  @Override
  public void register(Upgrade upgrade) {
    _upgrades.put(upgrade.id(), upgrade);
  }

  @Override
  public UpgradeResult execute(String upgradeId, String... args) {
    if (_upgrades.containsKey(upgradeId)) {
      Upgrade upgrade = _upgrades.get(upgradeId);
      UpgradeReport upgradeReport = new DefaultUpgradeReport();
      upgradeReport.addLine(String.format("Starting upgrade with id %s...", upgrade.id()));
      UpgradeResult result = executeInternal(upgrade, upgradeReport, args);
      upgradeReport.addLine(String.format("Upgrade %s completed with result %s. Exiting!", upgrade.id(), result.result()));
    }
    throw new IllegalArgumentException(String.format("No upgrade with id %s has been registered. Aborting...", upgradeId));
  }

  private UpgradeResult executeInternal(Upgrade upgrade, UpgradeReport upgradeReport, String... args) {

    List<UpgradeStep> steps = upgrade.steps();
    List<UpgradeStepResult> previousStepResults = new ArrayList<>();

    for (int i = 0; i < steps.size(); i++) {
      final UpgradeStep<?> step = steps.get(i);

      upgradeReport.addLine(String.format(String.format("Executing Step %s/%s: %s...", i + 1, steps.size(), step.id()), upgrade.id()));

      final UpgradeContext upgradeContext = new DefaultUpgradeContext(
          upgrade,
          upgradeReport,
          previousStepResults,
          args);

      final UpgradeStepResult<?> stepResult = executeStepInternal(upgradeContext, upgradeReport, step);
      upgradeReport.addLine(stepResult.message());
      previousStepResults.add(stepResult);

      if (UpgradeStepResult.Result.FAILED.equals(stepResult.result())) {

        if (step.isOptional()) {
          upgradeReport.addLine(String.format("Failed Step %s/%s: %s. Step marked as optional. Proceeding with upgrade...", i + 1, steps.size(), step.id()));
          continue;
        }

        // Required step failed. Fail the entire upgrade process.
        upgradeReport.addLine(
            String.format("Failed Step %s/%s: %s. Failed after %s retries.",
                i + 1,
                steps.size(),
                step.id(),
                step.retryCount()));
        upgradeReport.addLine(String.format("Exiting upgrade %s with failure.", upgrade.id()));
        return new DefaultUpgradeResult(UpgradeResult.Result.FAILED, upgradeReport);
      }

      upgradeReport.addLine(String.format("Completed Step %s/%s: %s successfully.", i + 1, steps.size(), step.id()));

      if (UpgradeStepResult.Action.ABORT.equals(stepResult.action())) {
        upgradeReport.addLine(String.format("Step with id %s requested an abort of the in-progress update. Aborting the upgrade...", step.id()));
        return new DefaultUpgradeResult(UpgradeResult.Result.ABORTED, upgradeReport);
      }
    }

    upgradeReport.addLine(String.format("Success! Completed upgrade with id %s successfully.", upgrade.id()));
    return new DefaultUpgradeResult(UpgradeResult.Result.SUCCEEDED, upgradeReport);
  }

  private UpgradeStepResult<?> executeStepInternal(UpgradeContext context, UpgradeReport upgradeReport, UpgradeStep<?> step) {
    int retryCount = step.retryCount();
    UpgradeStepResult<?> result = null;
    int maxAttempts = retryCount + 1;
    for (int i = 0; i < maxAttempts; i++) {
      try {
        result = step.executable().apply(context);

        if (result == null) {
          // Failed to even retrieve a result. Create a default failure result.
          result = new DefaultUpgradeStepResult<>(
              step.id(),
              UpgradeStepResult.Result.FAILED,
              String.format("Step %s returned null result.", step.id())
          );
          upgradeReport.addLine(String.format("Retrying %s more times...", maxAttempts - (i + 1)));
        }

        if (UpgradeStepResult.Result.SUCCEEDED.equals(result.result())) {
          break;
        } else {
          upgradeReport.addLine(result.message());
        }

      } catch (Exception e) {
        upgradeReport.addLine(String.format("Caught exception during retry %s of Step with id %s: %s",
            i,
            step.id(),
            e.toString()
        ));
        upgradeReport.addLine(String.format("Retrying %s more times...", maxAttempts - (i + 1)));
      }
    }

    return result;
  }
}