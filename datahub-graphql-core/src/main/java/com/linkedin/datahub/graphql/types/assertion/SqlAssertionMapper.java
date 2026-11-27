package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.SqlAssertionType;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnknownEnumValues;

public class SqlAssertionMapper extends AssertionMapper {

  public static com.linkedin.datahub.graphql.generated.SqlAssertionInfo mapSqlAssertionInfo(
      final SqlAssertionInfo gmsSqlAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.SqlAssertionInfo result =
        new com.linkedin.datahub.graphql.generated.SqlAssertionInfo();
    result.setEntityUrn(gmsSqlAssertionInfo.getEntity().toString());
    result.setType(
        UnknownEnumValues.require(SqlAssertionType.class, gmsSqlAssertionInfo.getType()));
    result.setStatement(gmsSqlAssertionInfo.getStatement());
    result.setOperator(
        UnknownEnumValues.require(AssertionStdOperator.class, gmsSqlAssertionInfo.getOperator()));
    result.setParameters(mapParameters(gmsSqlAssertionInfo.getParameters()));
    if (gmsSqlAssertionInfo.hasChangeType()) {
      result.setChangeType(
          UnknownEnumValues.mapOrNull(
              AssertionValueChangeType.class, gmsSqlAssertionInfo.getChangeType()));
    }
    return result;
  }

  private SqlAssertionMapper() {}
}
