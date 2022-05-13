package com.linkedin.metadata.entity.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class AspectStorageValidationUtil {

  private AspectStorageValidationUtil() {
  }

  public static boolean checkTableExists(CqlSession session) {
    String query = String.format("SELECT columnfamily_name\n "
        + "FROM schema_columnfamilies WHERE keyspace_name='%s';",
        CassandraAspect.TABLE_NAME);
    ResultSet rs = session.execute(query);
    return rs.all().size() > 0;
  }
}
