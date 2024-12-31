package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.common.urn.Urn;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SqlAssertionMapperTest {
  @Test
  public void testMapMetricSqlAssertionInfo() throws Exception {
    SqlAssertionInfo sqlAssertionInfo =
        new SqlAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(SqlAssertionType.METRIC)
            .setStatement("SELECT COUNT(*) FROM foo.bar.baz")
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setType(AssertionStdParameterType.NUMBER)
                            .setValue(("5"))));

    com.linkedin.datahub.graphql.generated.SqlAssertionInfo result =
        SqlAssertionMapper.mapSqlAssertionInfo(sqlAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.SqlAssertionType.METRIC);
    Assert.assertEquals(result.getStatement(), "SELECT COUNT(*) FROM foo.bar.baz");
    Assert.assertEquals(
        result.getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN);
    Assert.assertEquals(
        result.getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getParameters().getValue().getValue(), "5");
  }

  @Test
  public void testMapMetricChangeSqlAssertionInfo() throws Exception {
    SqlAssertionInfo sqlAssertionInfo =
        new SqlAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(SqlAssertionType.METRIC_CHANGE)
            .setStatement("SELECT COUNT(*) FROM foo.bar.baz")
            .setChangeType(AssertionValueChangeType.ABSOLUTE)
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setType(AssertionStdParameterType.NUMBER)
                            .setValue(("5"))));

    com.linkedin.datahub.graphql.generated.SqlAssertionInfo result =
        SqlAssertionMapper.mapSqlAssertionInfo(sqlAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.SqlAssertionType.METRIC_CHANGE);
    Assert.assertEquals(result.getStatement(), "SELECT COUNT(*) FROM foo.bar.baz");
    Assert.assertEquals(
        result.getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN);
    Assert.assertEquals(
        result.getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getParameters().getValue().getValue(), "5");
    Assert.assertEquals(
        result.getChangeType(),
        com.linkedin.datahub.graphql.generated.AssertionValueChangeType.ABSOLUTE);
  }
}
