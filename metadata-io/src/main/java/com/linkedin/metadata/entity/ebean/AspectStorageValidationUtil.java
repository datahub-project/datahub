package com.linkedin.metadata.entity.ebean;

import static io.ebean.Expr.ne;

import com.linkedin.metadata.Constants;
import io.ebean.Database;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import java.util.List;

public class AspectStorageValidationUtil {

  private AspectStorageValidationUtil() {}

  public static long getV1RowCount(Database server) {
    return server.find(EbeanAspectV1.class).findCount();
  }

  /**
   * Get the number of rows created not by the DataHub system actor
   * (urn:li:corpuser:__datahub_system)
   */
  public static long getV2NonSystemRowCount(Database server) {
    return server
        .find(EbeanAspectV2.class)
        .where(ne("createdby", Constants.SYSTEM_ACTOR))
        .findCount();
  }

  public static boolean checkV2TableExists(Database server) {
    final String queryStr =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES \n"
            + "WHERE lower(TABLE_NAME) = 'metadata_aspect_v2'";

    final SqlQuery query = server.sqlQuery(queryStr);
    final List<SqlRow> rows = query.findList();
    return rows.size() > 0;
  }

  public static boolean checkV1TableExists(Database server) {
    final String queryStr =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES \n"
            + "WHERE lower(TABLE_NAME) = 'metadata_aspect'";

    final SqlQuery query = server.sqlQuery(queryStr);
    final List<SqlRow> rows = query.findList();
    return rows.size() > 0;
  }
}
