package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import io.ebean.Database;
import java.util.function.Function;

public class CreateAspectTableStep implements UpgradeStep {

  private static final String DB_TYPE_ARG = "dbType";

  enum DbType {
    MYSQL,
    POSTGRES,
    MARIA
  }

  private final Database _server;

  public CreateAspectTableStep(final Database server) {
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
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      DbType targetDbType =
          context.parsedArgs().containsKey(DB_TYPE_ARG)
              ? DbType.valueOf(context.parsedArgs().get(DB_TYPE_ARG).get())
              : DbType.MYSQL;

      String sqlUpdateStr;

      switch (targetDbType) {
        case POSTGRES:
          sqlUpdateStr =
              "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (\n"
                  + "  urn                           varchar(500) not null,\n"
                  + "  aspect                        varchar(200) not null,\n"
                  + "  version                       bigint not null,\n"
                  + "  metadata                      text not null,\n"
                  + "  systemmetadata                text,\n"
                  + "  createdon                     timestamp not null,\n"
                  + "  createdby                     varchar(255) not null,\n"
                  + "  createdfor                    varchar(255),\n"
                  + "  constraint pk_metadata_aspect_v2 primary key (urn,aspect,version)\n"
                  + ")";
          break;
        default:
          // both mysql and maria
          sqlUpdateStr =
              "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (\n"
                  + "  urn                           varchar(500) not null,\n"
                  + "  aspect                        varchar(200) not null,\n"
                  + "  version                       bigint(20) not null,\n"
                  + "  metadata                      longtext not null,\n"
                  + "  systemmetadata                longtext,\n"
                  + "  createdon                     datetime(6) not null,\n"
                  + "  createdby                     varchar(255) not null,\n"
                  + "  createdfor                    varchar(255),\n"
                  + "  constraint pk_metadata_aspect_v2 primary key (urn,aspect,version)\n"
                  + ")";
          break;
      }

      try {
        _server.execute(_server.createSqlUpdate(sqlUpdateStr));
      } catch (Exception e) {
        context.report().addLine("Failed to create table metadata_aspect_v2", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
