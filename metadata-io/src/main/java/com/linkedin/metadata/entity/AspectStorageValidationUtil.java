package com.linkedin.metadata.entity;

import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;


public class AspectStorageValidationUtil {
  private AspectStorageValidationUtil() {

  }
  public static boolean checkV2TableExists(EbeanServer server) {
    final String queryStr =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES \n"
            + "WHERE TABLE_NAME = 'metadata_aspect_v2'";

    final SqlQuery query = server.createSqlQuery(queryStr);
    SqlRow row = query.findOne();
    return row != null;
  }
}
