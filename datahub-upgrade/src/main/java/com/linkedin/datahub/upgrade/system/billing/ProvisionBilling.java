package com.linkedin.datahub.upgrade.system.billing;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * System upgrade that provisions billing for all customers.
 *
 * <p>This upgrade creates a customer and contract in the billing provider. It only runs if:
 *
 * <ul>
 *   <li>Billing is enabled
 *   <li>Billing handler is available
 * </ul>
 *
 * <p>The upgrade is idempotent at the customer level and will skip customer creation if the
 * customer already exists.
 */
@Slf4j
public class ProvisionBilling implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public ProvisionBilling(
      OperationContext opContext,
      EntityService<?> entityService,
      BillingHandler billingHandler,
      boolean reprocessEnabled) {

    if (billingHandler != null && billingHandler.isEnabled()) {
      steps =
          ImmutableList.of(
              new ProvisionBillingStep(opContext, entityService, billingHandler, reprocessEnabled));
    } else {
      log.info("Billing is not enabled or not configured, skipping ProvisionBilling upgrade");
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
