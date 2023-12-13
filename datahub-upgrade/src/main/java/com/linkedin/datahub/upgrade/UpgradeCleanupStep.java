package com.linkedin.datahub.upgrade;

import java.util.function.BiConsumer;

/**
 * Step executed on finish of an {@link Upgrade}.
 *
 * <p>Note that this step is not retried, even in case of failures.
 */
public interface UpgradeCleanupStep {
  /** Returns an identifier for the upgrade step. */
  String id();

  /** Returns a function representing the cleanup step's logic. */
  BiConsumer<UpgradeContext, UpgradeResult> executable();
}
