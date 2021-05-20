package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import java.util.function.Function;


public class UpgradeQualificationStep implements UpgradeStep<Void> {

  private final EbeanServer _server;

  UpgradeQualificationStep(EbeanServer server) {
    _server = server;
  }

  @Override
  public String id() {
    return "UpgradeQualificationStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult<Void>> executable() {
    return (context) -> {
      final String queryStr =
          "SELECT * FROM INFORMATION_SCHEMA.TABLES \n"
          + "WHERE TABLE_NAME = 'metadata_aspect_v2'";

      final SqlQuery query = _server.createSqlQuery(queryStr);
      try {
        SqlRow row = query.findOne();

        if (row == null) {
          // Qualified.
          return new DefaultUpgradeStepResult<>(
              id(),
              UpgradeStepResult.Result.SUCCEEDED,
              "Found qualified upgrade candidate. Proceeding with upgrade...");
        }

        // Unqualified (Table already exists)
        return new DefaultUpgradeStepResult<>(
            id(),
            UpgradeStepResult.Result.SUCCEEDED,
            UpgradeStepResult.Action.ABORT,
            "Failed to qualify upgrade candidate. Aborting the upgrade...");

      } catch (Exception e) {
        return new DefaultUpgradeStepResult<>(
            id(),
            UpgradeStepResult.Result.FAILED,
            String.format("Failed to execute SQL query: %s", queryStr));
      }
    };
  }
}
