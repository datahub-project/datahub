/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import javax.annotation.Nonnull;

public class AspectStorageValidationUtil {

  private AspectStorageValidationUtil() {}

  /**
   * Check if entity aspect table exists in the database.
   *
   * @param session
   * @return {@code true} if table exists.
   */
  public static boolean checkTableExists(@Nonnull CqlSession session) {
    String query =
        String.format(
            "SELECT table_name \n "
                + "FROM system_schema.tables where table_name = '%s' allow filtering;",
            CassandraAspect.TABLE_NAME);
    ResultSet rs = session.execute(query);
    return rs.all().size() > 0;
  }
}
