package com.linkedin.datahub.upgrade;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/** Responsible for managing the execution of an {@link Upgrade}. */
public interface UpgradeManager {

  /** Register an {@link Upgrade} with the manaager. */
  UpgradeManager register(Upgrade upgrade);

  /** Kick off an {@link Upgrade} by identifier. */
  UpgradeResult execute(
      @Nonnull OperationContext systemOpContext, String upgradeId, List<String> args);
}
