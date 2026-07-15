package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.datahubusage.UsageEventsInfrastructureProvisioner;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CreateUsageEventIndicesStep implements UpgradeStep {
  private final ConfigurationProvider configurationProvider;
  private final UsageEventsInfrastructureProvisioner usageEventsInfrastructureProvisioner;

  @Override
  public String id() {
    return "CreateUsageEventIndicesStep";
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    boolean skipViaEnvVar =
        EnvironmentUtils.getBoolean("SKIP_CREATE_USAGE_EVENT_INDICES_STEP", false);
    if (skipViaEnvVar) {
      log.info(
          "Environment variable SKIP_CREATE_USAGE_EVENT_INDICES_STEP is set to true. Skipping usage event storage setup.");
      return true;
    }

    boolean analyticsEnabled = configurationProvider.getPlatformAnalytics().isEnabled();
    if (!analyticsEnabled) {
      log.info("DataHub analytics is disabled, skipping usage event storage setup");
      return true;
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        usageEventsInfrastructureProvisioner.provision(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("CreateUsageEventIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }
}
