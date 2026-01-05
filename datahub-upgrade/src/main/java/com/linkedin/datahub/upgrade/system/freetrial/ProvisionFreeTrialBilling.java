package com.linkedin.datahub.upgrade.system.freetrial;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * System upgrade that provisions billing for free trial instances.
 *
 * <p>This upgrade creates a customer and free trial contract in the billing provider (Metronome).
 * It only runs if:
 *
 * <ul>
 *   <li>Billing is enabled
 *   <li>Instance is configured as a free trial
 *   <li>Billing handler is available
 * </ul>
 *
 * <p>The upgrade is idempotent and will skip if the customer already exists.
 */
@Slf4j
public class ProvisionFreeTrialBilling implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public ProvisionFreeTrialBilling(
      OperationContext opContext,
      EntityService<?> entityService,
      BillingHandler billingHandler,
      DataHubConfiguration dataHubConfiguration,
      boolean reprocessEnabled) {

    if (billingHandler != null && billingHandler.isEnabled() && dataHubConfiguration != null) {
      steps =
          ImmutableList.of(
              new ProvisionFreeTrialBillingStep(
                  opContext,
                  entityService,
                  billingHandler,
                  dataHubConfiguration,
                  reprocessEnabled));
    } else {
      log.info(
          "Billing is not enabled or not configured, skipping ProvisionFreeTrialBilling upgrade");
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
