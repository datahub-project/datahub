package com.linkedin.datahub.upgrade;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Specification of an upgrade to be performed to the DataHub platform. */
public interface Upgrade {

  /** String identifier for the upgrade. */
  String id();

  /** Returns a set of steps to perform during the upgrade. */
  List<UpgradeStep> steps();

  /** Returns a set of steps to perform on upgrade success, failure, or abort. */
  default List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }

  default Optional<DataHubUpgradeResult> getUpgradeResult(
      @Nonnull OperationContext opContext, Urn upgradeId, EntityService<?> entityService) {
    return BootstrapStep.getUpgradeResult(opContext, upgradeId, entityService);
  }

  default void setUpgradeResult(
      @Nonnull OperationContext opContext,
      @Nonnull Urn upgradeId,
      EntityService<?> entityService,
      @Nullable DataHubUpgradeState state,
      @Nullable Map<String, String> result) {
    BootstrapStep.setUpgradeResult(opContext, upgradeId, entityService, state, result);
  }
}
