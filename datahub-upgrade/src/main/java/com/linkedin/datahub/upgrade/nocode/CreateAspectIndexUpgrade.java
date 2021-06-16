package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class CreateAspectIndexUpgrade implements Upgrade {

  private final List<UpgradeStep> _steps;

  // Upgrade requires the EbeanServer.
  public CreateAspectIndexUpgrade(final EbeanServer server) {
    _steps = buildUpgradeSteps(server);
  }

  @Override
  public String id() {
    return "CreateAspectIndexUpgrade";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return Collections.emptyList();
  }

  private List<UpgradeStep> buildUpgradeSteps(final EbeanServer server) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new CreateAspectTableStep(server));
    return steps;
  }
}
