/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.removeunknownaspects;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import java.util.ArrayList;
import java.util.List;

public class RemoveUnknownAspects implements Upgrade {

  private final List<UpgradeStep> _steps;

  public RemoveUnknownAspects(final EntityService<?> entityService) {
    _steps = buildSteps(entityService);
  }

  @Override
  public String id() {
    return this.getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EntityService<?> entityService) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new RemoveClientIdAspectStep(entityService));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
