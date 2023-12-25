package com.linkedin.datahub.upgrade.impl;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeManager;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class DefaultUpgradeManager implements UpgradeManager {

  private final Map<String, Upgrade> _upgrades = new HashMap<>();

  @Override
  public void register(@Nonnull Upgrade upgrade) {
    _upgrades.put(upgrade.id(), upgrade);
  }

  @Override
  public UpgradeResult execute(String upgradeId, List<String> args) {
    if (_upgrades.containsKey(upgradeId)) {
      return executeInternal(_upgrades.get(upgradeId), args);
    }
    throw new IllegalArgumentException(
        String.format("No upgrade with id %s could be found. Aborting...", upgradeId));
  }

  private UpgradeResult executeInternal(Upgrade upgrade, List<String> args) {
    final UpgradeReport upgradeReport = new DefaultUpgradeReport();
    final UpgradeContext context =
        new DefaultUpgradeContext(upgrade, upgradeReport, new ArrayList<>(), args);
    upgradeReport.addLine(String.format("Starting upgrade with id %s...", upgrade.id()));
    UpgradeResult result = executeInternal(context);
    upgradeReport.addLine(
        String.format(
            "Upgrade %s completed with result %s. Exiting...", upgrade.id(), result.result()));
    executeCleanupInternal(context, result);
    return result;
  }

  private UpgradeResult executeInternal(UpgradeContext context) {

    final Upgrade upgrade = context.upgrade();
    final List<UpgradeStep> steps = context.upgrade().steps();
    final List<UpgradeStepResult> stepResults = context.stepResults();
    final UpgradeReport upgradeReport = context.report();

    for (int i = 0; i < steps.size(); i++) {
      final UpgradeStep step = steps.get(i);

      if (step.skip(context)) {
        upgradeReport.addLine(
            String.format(
                String.format("Skipping Step %s/%s: %s...", i + 1, steps.size(), step.id()),
                upgrade.id()));
        continue;
      }

      upgradeReport.addLine(
          String.format(
              String.format("Executing Step %s/%s: %s...", i + 1, steps.size(), step.id()),
              upgrade.id()));

      final UpgradeStepResult stepResult = executeStepInternal(context, step);
      stepResults.add(stepResult);

      // Apply Actions
      if (UpgradeStepResult.Action.ABORT.equals(stepResult.action())) {
        upgradeReport.addLine(
            String.format(
                "Step with id %s requested an abort of the in-progress update. Aborting the upgrade...",
                step.id()));
        return new DefaultUpgradeResult(UpgradeResult.Result.ABORTED, upgradeReport);
      }

      // Handle Results
      if (UpgradeStepResult.Result.FAILED.equals(stepResult.result())) {
        if (step.isOptional()) {
          upgradeReport.addLine(
              String.format(
                  "Failed Step %s/%s: %s. Step marked as optional. Proceeding with upgrade...",
                  i + 1, steps.size(), step.id()));
          continue;
        }

        // Required step failed. Fail the entire upgrade process.
        upgradeReport.addLine(
            String.format(
                "Failed Step %s/%s: %s. Failed after %s retries.",
                i + 1, steps.size(), step.id(), step.retryCount()));
        upgradeReport.addLine(String.format("Exiting upgrade %s with failure.", upgrade.id()));
        return new DefaultUpgradeResult(UpgradeResult.Result.FAILED, upgradeReport);
      }

      upgradeReport.addLine(
          String.format("Completed Step %s/%s: %s successfully.", i + 1, steps.size(), step.id()));
    }

    upgradeReport.addLine(
        String.format("Success! Completed upgrade with id %s successfully.", upgrade.id()));
    return new DefaultUpgradeResult(UpgradeResult.Result.SUCCEEDED, upgradeReport);
  }

  private UpgradeStepResult executeStepInternal(UpgradeContext context, UpgradeStep step) {
    int retryCount = step.retryCount();
    UpgradeStepResult result = null;
    int maxAttempts = retryCount + 1;
    for (int i = 0; i < maxAttempts; i++) {
      try (Timer.Context completionTimer =
          MetricUtils.timer(MetricRegistry.name(step.id(), "completionTime")).time()) {
        try (Timer.Context executionTimer =
            MetricUtils.timer(MetricRegistry.name(step.id(), "executionTime")).time()) {
          result = step.executable().apply(context);
        }

        if (result == null) {
          // Failed to even retrieve a result. Create a default failure result.
          result = new DefaultUpgradeStepResult(step.id(), UpgradeStepResult.Result.FAILED);
          context
              .report()
              .addLine(String.format("Retrying %s more times...", maxAttempts - (i + 1)));
          MetricUtils.counter(MetricRegistry.name(step.id(), "retry")).inc();
        }

        if (UpgradeStepResult.Result.SUCCEEDED.equals(result.result())) {
          MetricUtils.counter(MetricRegistry.name(step.id(), "succeeded")).inc();
          break;
        }
      } catch (Exception e) {
        context
            .report()
            .addLine(
                String.format(
                    "Caught exception during attempt %s of Step with id %s: %s", i, step.id(), e));
        MetricUtils.counter(MetricRegistry.name(step.id(), "failed")).inc();
        result = new DefaultUpgradeStepResult(step.id(), UpgradeStepResult.Result.FAILED);
        context.report().addLine(String.format("Retrying %s more times...", maxAttempts - (i + 1)));
      }
    }

    return result;
  }

  private void executeCleanupInternal(UpgradeContext context, UpgradeResult result) {
    for (UpgradeCleanupStep step : context.upgrade().cleanupSteps()) {
      try {
        step.executable().accept(context, result);
      } catch (Exception e) {
        context
            .report()
            .addLine(
                String.format(
                    "Caught exception while executing cleanup step with id %s", step.id()));
      }
    }
  }
}
