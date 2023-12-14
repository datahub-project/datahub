package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.CreateSqlAssertionInput;
import javax.annotation.Nonnull;

public class SqlAssertionUtils {
  @Nonnull
  public static com.linkedin.assertion.SqlAssertionInfo createSqlAssertionInfo(
      @Nonnull final CreateSqlAssertionInput input) {
    final com.linkedin.assertion.SqlAssertionInfo result =
        new com.linkedin.assertion.SqlAssertionInfo();
    final Urn asserteeUrn = UrnUtils.getUrn(input.getEntityUrn());

    result.setEntity(asserteeUrn);
    result.setType(SqlAssertionType.valueOf(input.getType().toString()));
    result.setStatement(input.getStatement());
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    if (input.getType() == com.linkedin.datahub.graphql.generated.SqlAssertionType.METRIC_CHANGE) {
      if (input.getChangeType() != null) {
        result.setChangeType(AssertionValueChangeType.valueOf(input.getChangeType().toString()));
      } else {
        throw new IllegalArgumentException(
            "Change type must be specified for METRIC_CHANGE assertion");
      }
    }

    return result;
  }

  private SqlAssertionUtils() {}
}
