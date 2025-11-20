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
public class CreateCdcUserStep implements UpgradeStep {

  private final Database server;
  private final SqlSetupArgs setupArgs;
  private final DatabaseOperations dbOps;

  public CreateCdcUserStep(final Database server, final SqlSetupArgs setupArgs) {
    this.server = server;
    this.setupArgs = setupArgs;
    this.dbOps = DatabaseOperations.create(setupArgs.getDbType());
  }

  @Override
  public String id() {
    return "CreateCdcUserStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Creating CDC user...");

        SqlSetupResult result = createCdcUser(setupArgs);

        if (result.isCdcUserCreated()) {
          context
              .report()
              .addLine(String.format("CDC user '%s' created successfully", setupArgs.getCdcUser()));
        } else {
          context.report().addLine("CDC user creation skipped or failed");
        }
        context
            .report()
            .addLine(String.format("Execution time: %d ms", result.getExecutionTimeMs()));

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Error during CreateCdcUserStep execution", e);
        context.report().addLine(String.format("Error during execution: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  SqlSetupResult createCdcUser(SqlSetupArgs args) throws SQLException {
    SqlSetupResult result = new SqlSetupResult();
    long startTime = System.currentTimeMillis();

    if (!args.isCdcEnabled()) {
      log.info("CDC is not enabled, skipping CDC user creation");
      return result;
    }

    try {
      try (Connection connection = server.dataSource().getConnection()) {
        String createCdcUserSql = dbOps.createCdcUserSql(args.getCdcUser(), args.getCdcPassword());
        try (PreparedStatement stmt = connection.prepareStatement(createCdcUserSql)) {
          stmt.executeUpdate();
        }

        java.util.List<String> grantCdcPrivilegesStmts =
            dbOps.grantCdcPrivilegesSql(args.getCdcUser(), args.getDatabaseName());
        for (String grantSql : grantCdcPrivilegesStmts) {
          try (PreparedStatement grantStmt = connection.prepareStatement(grantSql)) {
            grantStmt.executeUpdate();
          }
        }

        result.setCdcUserCreated(true);
        log.info("CDC user '{}' created successfully", args.getCdcUser());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create CDC user: " + e.getMessage(), e);
    }

    result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
    return result;
  }

  public boolean containsKey(
      java.util.Map<String, java.util.Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key)
        && parsedArgs.get(key) != null
        && parsedArgs.get(key).isPresent();
  }
}
