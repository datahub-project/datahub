package com.linkedin.datahub.upgrade.system.freetrial;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.BillingConfiguration;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Upgrade step to provision billing for free trial instances.
 *
 * <p>This step provisions a customer in the billing provider (Metronome) with a package-based
 * contract. It only runs once per instance and is idempotent - if the customer already exists, it
 * will skip provisioning.
 *
 * <p>The step will fail (and retry on next upgrade) if:
 *
 * <ul>
 *   <li>Billing is not enabled
 *   <li>Instance is not configured as a free trial
 *   <li>Billing configuration is incomplete
 *   <li>Billing provider API is unavailable
 * </ul>
 *
 * <p>The step will succeed (and not run again) if:
 *
 * <ul>
 *   <li>Customer is successfully provisioned
 *   <li>Customer already exists in billing provider
 * </ul>
 */
@Slf4j
public class ProvisionFreeTrialBillingStep implements UpgradeStep {

  private static final String UPGRADE_ID = "ProvisionFreeTrialBilling";
  private static final com.linkedin.common.urn.Urn UPGRADE_ID_URN =
      BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final BillingHandler billingHandler;
  private final DataHubConfiguration dataHubConfiguration;
  private final boolean reprocessEnabled;

  public ProvisionFreeTrialBillingStep(
      @Nonnull OperationContext systemOpContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull BillingHandler billingHandler,
      @Nonnull DataHubConfiguration dataHubConfiguration,
      boolean reprocessEnabled) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.billingHandler = billingHandler;
    this.dataHubConfiguration = dataHubConfiguration;
    this.reprocessEnabled = reprocessEnabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        log.info("Starting ProvisionFreeTrialBilling upgrade step");

        // Check if billing is enabled
        if (!billingHandler.isEnabled()) {
          log.info("Billing is not enabled, will retry on next upgrade");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        // Validate free trial instance
        if (!dataHubConfiguration.isFreeTrialInstance()) {
          log.info("Not a free trial instance, will retry on next upgrade");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        // Validate billing configuration has a package alias
        BillingConfiguration billingConfig = dataHubConfiguration.getBilling();
        if (billingConfig == null
            || billingConfig.getMetronome() == null
            || billingConfig.getMetronome().getPackageAlias() == null
            || billingConfig.getMetronome().getPackageAlias().trim().isEmpty()) {
          log.warn("Billing package alias is not configured, will retry on next upgrade");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        String packageAlias = billingConfig.getMetronome().getPackageAlias();

        // Provision customer with package (idempotent)
        billingHandler.provisionCustomer(packageAlias);

        // Mark upgrade as complete
        BootstrapStep.setUpgradeResult(systemOpContext, UPGRADE_ID_URN, entityService);

        log.info("Successfully provisioned billing customer with package '{}'", packageAlias);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error(
            "Failed to provision billing customer, will retry on next upgrade: {}",
            e.getMessage(),
            e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      log.info("ProvisionFreeTrialBilling reprocess enabled. Running.");
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }

    return previouslyRun;
  }
}
