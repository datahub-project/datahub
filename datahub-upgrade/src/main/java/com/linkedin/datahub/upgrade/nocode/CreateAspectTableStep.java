package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import java.util.function.Function;

// Do we need SQL-tech specific migration paths?
public class CreateAspectTableStep implements UpgradeStep<Void> {

  private final EbeanServer _server;

  public CreateAspectTableStep(final EbeanServer server) {
    _server = server;
  }

  @Override
  public String id() {
    return "CreateAspectTableStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult<Void>> executable() {
    return (context) -> {
      final SqlQuery query = _server.createSqlQuery(
          "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (\n"
              + "  urn                           varchar(500) not null,\n"
              + "  aspect                        varchar(200) not null,\n"
              + "  version                       bigint(20) not null,\n"
              + "  metadata                      longtext not null,\n"
              + "  createdon                     datetime(6) not null,\n"
              + "  createdby                     varchar(255) not null,\n"
              + "  createdfor                    varchar(255),\n"
              + "  constraint pk_metadata_aspect primary key (urn,aspect,version)\n"
              + ")"
      );
      try {
        query.findList();
      } catch (Exception e) {
        return new DefaultUpgradeStepResult<>(
            id(),
            UpgradeStepResult.Result.FAILED,
            String.format("Failed to create table metadata_aspect_v2: %s", e.toString()));
      }
      return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
