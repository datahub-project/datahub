package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Upgrade implementation for SQL database setup operations. Configures database tables, users, and
 * CDC functionality based on provided arguments.
 */
public class SqlSetup implements Upgrade {
  private final List<UpgradeStep> _steps;

  /**
   * Constructs a SqlSetup upgrade with the specified database server and configuration.
   *
   * @param server the database server instance, or null to create an empty upgrade
   * @param setupArgs the SQL setup configuration arguments, or null to create an empty upgrade
   */
  public SqlSetup(@Nullable final Database server, @Nullable final SqlSetupArgs setupArgs) {
    if (server != null && setupArgs != null) {
      _steps = buildSteps(server, setupArgs);
    } else {
      _steps = List.of();
    }
  }

  /**
   * Returns the unique identifier for this upgrade.
   *
   * @return the upgrade ID "SqlSetup"
   */
  @Override
  public String id() {
    return "SqlSetup";
  }

  /**
   * Returns the list of upgrade steps to be executed.
   *
   * @return the list of upgrade steps
   */
  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final Database server, final SqlSetupArgs setupArgs) {
    final List<UpgradeStep> steps = new ArrayList<>();

    // Add database table creation step
    steps.add(new CreateTablesStep(server, setupArgs));

    // Add user creation step if enabled
    if (setupArgs.isCreateUser()) {
      steps.add(new CreateUsersStep(server, setupArgs));
    }

    // Add CDC user creation step if enabled
    if (setupArgs.isCdcEnabled()) {
      steps.add(new CreateCdcUserStep(server, setupArgs));
    }

    return steps;
  }

  /**
   * Returns the list of cleanup steps for this upgrade. SqlSetup does not require any cleanup
   * operations.
   *
   * @return an empty list of cleanup steps
   */
  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return List.of();
  }
}
