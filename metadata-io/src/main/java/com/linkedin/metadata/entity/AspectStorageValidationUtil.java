package com.linkedin.metadata.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectV1;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import java.util.List;


public class AspectStorageValidationUtil {

  private AspectStorageValidationUtil() {

  }

  public static long getV1RowCount(EbeanServer server) {
    return server.find(EbeanAspectV1.class).findCount();
  }

  public static long getV2RowCount(EbeanServer server) {
    return server.find(EbeanAspectV2.class).findCount();
  }

  public static boolean checkV2TableExists(EbeanServer server) {
    final String queryStr =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES \n"
            + "WHERE TABLE_NAME = 'metadata_aspect_v2'";

    final SqlQuery query = server.createSqlQuery(queryStr);
    final List<SqlRow> rows = query.findList();
    return rows.size() > 0;
  }

  public static boolean checkV1TableExists(EbeanServer server) {
    final String queryStr =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES \n"
            + "WHERE TABLE_NAME = 'metadata_aspect'";

    final SqlQuery query = server.createSqlQuery(queryStr);
    final List<SqlRow> rows = query.findList();
    return rows.size() > 0;
  }
}
