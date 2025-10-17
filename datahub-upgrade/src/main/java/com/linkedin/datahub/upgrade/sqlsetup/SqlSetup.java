package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class SqlSetup implements Upgrade {
  private final List<UpgradeStep> _steps;

  public SqlSetup(@Nullable final Database server, @Nullable final SqlSetupArgs setupArgs) {
    if (server != null && setupArgs != null) {
      _steps = buildSteps(server, setupArgs);
    } else {
      _steps = List.of();
    }
  }

  @Override
  public String id() {
    return "SqlSetup";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final Database server, final SqlSetupArgs setupArgs) {
    final List<UpgradeStep> steps = new ArrayList<>();

    // Add database table creation step
    steps.add(new CreateTablesStep(server, setupArgs));

    // Add user creation step if enabled
    if (setupArgs.createUser) {
      steps.add(new CreateUsersStep(server, setupArgs));
    }

    // Add CDC user creation step if enabled
    if (setupArgs.cdcEnabled) {
      steps.add(new CreateCdcUserStep(server, setupArgs));
    }

    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return List.of();
  }
}
