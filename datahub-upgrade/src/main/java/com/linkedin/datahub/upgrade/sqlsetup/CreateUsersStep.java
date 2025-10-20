package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateUsersStep implements UpgradeStep {

  private final Database server;
  private final SqlSetupArgs setupArgs;
  private final DatabaseOperations dbOps;

  public CreateUsersStep(final Database server, final SqlSetupArgs setupArgs) {
    this.server = server;
    this.setupArgs = setupArgs;
    this.dbOps = DatabaseOperations.create(setupArgs.getDbType());
  }

  @Override
  public String id() {
    return "CreateUsersStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Creating database users...");

        SqlSetupResult result = createUsers(setupArgs);

        context.report().addLine(String.format("Users created: %d", result.getUsersCreated()));
        context
            .report()
            .addLine(String.format("Execution time: %d ms", result.getExecutionTimeMs()));

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Error during CreateUsersStep execution", e);
        context.report().addLine(String.format("Error during execution: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  SqlSetupResult createUsers(SqlSetupArgs args) throws SQLException {
    SqlSetupResult result = new SqlSetupResult();
    long startTime = System.currentTimeMillis();

    // Only create users if explicitly requested (matching original CREATE_USER logic)
    if (!args.isCreateUser()) {
      log.info("User creation not requested, skipping");
      return result;
    }

    if (args.isIamAuthEnabled()) {
      createIamUser(args, result);
    } else {
      createTraditionalUser(args, result);
    }

    result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
    return result;
  }

  void createIamUser(SqlSetupArgs args, SqlSetupResult result) throws SQLException {
    log.info("Creating IAM-authenticated user: {}", args.getCreateUserUsername());

    try (Connection connection = server.dataSource().getConnection()) {
      String createUserSql =
          dbOps.createIamUserSql(args.getCreateUserUsername(), args.getCreateUserIamRole());
      try (PreparedStatement stmt = connection.prepareStatement(createUserSql)) {
        stmt.executeUpdate();
      }

      String grantPrivilegesSql =
          dbOps.grantPrivilegesSql(args.getCreateUserUsername(), args.getDatabaseName());
      try (PreparedStatement grantStmt = connection.prepareStatement(grantPrivilegesSql)) {
        grantStmt.executeUpdate();
      }

      result.setUsersCreated(1);
      log.info("IAM user '{}' created successfully", args.getCreateUserUsername());
    }
  }

  void createTraditionalUser(SqlSetupArgs args, SqlSetupResult result) throws SQLException {
    log.info("Creating traditional user: {}", args.getCreateUserUsername());

    try (Connection connection = server.dataSource().getConnection()) {
      String createUserSql =
          dbOps.createTraditionalUserSql(
              args.getCreateUserUsername(), args.getCreateUserPassword());
      try (PreparedStatement stmt = connection.prepareStatement(createUserSql)) {
        stmt.executeUpdate();
      }

      String grantPrivilegesSql =
          dbOps.grantPrivilegesSql(args.getCreateUserUsername(), args.getDatabaseName());
      try (PreparedStatement grantStmt = connection.prepareStatement(grantPrivilegesSql)) {
        grantStmt.executeUpdate();
      }

      result.setUsersCreated(1);
      log.info("Traditional user '{}' created successfully", args.getCreateUserUsername());
    }
  }

  public boolean containsKey(
      java.util.Map<String, java.util.Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key)
        && parsedArgs.get(key) != null
        && parsedArgs.get(key).isPresent();
  }
}
