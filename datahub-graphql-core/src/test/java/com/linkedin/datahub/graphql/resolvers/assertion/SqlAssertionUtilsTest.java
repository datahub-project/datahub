package com.linkedin.datahub.graphql.resolvers.assertion;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.CreateSqlAssertionInput;
import com.linkedin.datahub.graphql.generated.SqlAssertionType;

public class SqlAssertionUtilsTest {
    @Test
    public void testCreateSqlAssertionInfoMetric() {
        CreateSqlAssertionInput input = new CreateSqlAssertionInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();

        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        input.setEntityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,dataset1,PROD)");
        input.setType(SqlAssertionType.METRIC);
        input.setStatement("SELECT COUNT(*) FROM dataset1");
        input.setOperator(AssertionStdOperator.GREATER_THAN);
        input.setParameters(parameters);

        final com.linkedin.assertion.SqlAssertionInfo result = SqlAssertionUtils.createSqlAssertionInfo(input);
        Assert.assertEquals(result.getType(), com.linkedin.assertion.SqlAssertionType.METRIC);
        Assert.assertEquals(result.getStatement(), "SELECT COUNT(*) FROM dataset1");
        Assert.assertEquals(result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
    }

    @Test
    public void testCreateSqlAssertionInfoMetricChange() {
        CreateSqlAssertionInput input = new CreateSqlAssertionInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();

        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        input.setEntityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,dataset1,PROD)");
        input.setType(SqlAssertionType.METRIC_CHANGE);
        input.setStatement("SELECT COUNT(*) FROM dataset1");
        input.setOperator(AssertionStdOperator.GREATER_THAN);
        input.setParameters(parameters);
        input.setChangeType(AssertionValueChangeType.ABSOLUTE);

        final com.linkedin.assertion.SqlAssertionInfo result = SqlAssertionUtils.createSqlAssertionInfo(input);
        Assert.assertEquals(result.getType(), com.linkedin.assertion.SqlAssertionType.METRIC_CHANGE);
        Assert.assertEquals(result.getChangeType(), com.linkedin.assertion.AssertionValueChangeType.ABSOLUTE);
        Assert.assertEquals(result.getStatement(), "SELECT COUNT(*) FROM dataset1");
        Assert.assertEquals(result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
    }
}
