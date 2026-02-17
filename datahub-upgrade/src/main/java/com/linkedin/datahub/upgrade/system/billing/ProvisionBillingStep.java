package com.linkedin.datahub.upgrade.system.billing;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Upgrade step to provision billing for all customers.
 *
 * <p>This step provisions a customer in the billing provider with the appropriate contract. It only
 * runs once per instance and is idempotent - if the customer already exists, it will skip customer
 * creation.
 *
 * <p>Provider-specific details (e.g., Metronome package alias) are handled internally by the
 * billing provider and do not need to be configured at this level.
 *
 * <p>The step will fail (and retry on next upgrade) if:
 *
 * <ul>
 *   <li>Billing is not enabled
 *   <li>Billing provider configuration is incomplete
 *   <li>Billing provider API is unavailable
 * </ul>
 *
 * <p>The step will succeed (and not run again) if:
 *
 * <ul>
 *   <li>Customer is successfully provisioned
 * </ul>
 */
@Slf4j
public class ProvisionBillingStep implements UpgradeStep {

  private static final String UPGRADE_ID = "ProvisionBilling";
  private static final com.linkedin.common.urn.Urn UPGRADE_ID_URN =
      BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final BillingHandler billingHandler;
  private final boolean reprocessEnabled;

  public ProvisionBillingStep(
      @Nonnull OperationContext systemOpContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull BillingHandler billingHandler,
      boolean reprocessEnabled) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.billingHandler = billingHandler;
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
        log.info("Starting ProvisionBilling upgrade step");

        // Check if billing is enabled
        if (!billingHandler.isEnabled()) {
          log.info("Billing is not enabled, will retry on next upgrade");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        // Provision customer (provider handles its own config validation)
        billingHandler.provisionCustomer();

        // Mark upgrade as complete
        BootstrapStep.setUpgradeResult(systemOpContext, UPGRADE_ID_URN, entityService);

        log.info("Successfully provisioned billing customer");
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
      log.info("ProvisionBilling reprocess enabled. Running.");
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
