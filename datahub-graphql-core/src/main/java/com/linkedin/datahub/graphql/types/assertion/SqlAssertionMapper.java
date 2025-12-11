/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.SqlAssertionType;

public class SqlAssertionMapper extends AssertionMapper {

  public static com.linkedin.datahub.graphql.generated.SqlAssertionInfo mapSqlAssertionInfo(
      final SqlAssertionInfo gmsSqlAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.SqlAssertionInfo result =
        new com.linkedin.datahub.graphql.generated.SqlAssertionInfo();
    result.setEntityUrn(gmsSqlAssertionInfo.getEntity().toString());
    result.setType(SqlAssertionType.valueOf(gmsSqlAssertionInfo.getType().name()));
    result.setStatement(gmsSqlAssertionInfo.getStatement());
    result.setOperator(AssertionStdOperator.valueOf(gmsSqlAssertionInfo.getOperator().name()));
    result.setParameters(mapParameters(gmsSqlAssertionInfo.getParameters()));
    if (gmsSqlAssertionInfo.hasChangeType()) {
      result.setChangeType(
          AssertionValueChangeType.valueOf(gmsSqlAssertionInfo.getChangeType().name()));
    }
    return result;
  }

  private SqlAssertionMapper() {}
}
