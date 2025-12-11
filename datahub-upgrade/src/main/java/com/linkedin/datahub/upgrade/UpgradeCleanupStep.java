/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
