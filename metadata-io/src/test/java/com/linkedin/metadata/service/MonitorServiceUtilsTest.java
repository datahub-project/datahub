package com.linkedin.metadata.service;

import org.testng.annotations.Test;

import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FieldTransform;
import com.linkedin.assertion.FieldTransformType;
import com.linkedin.assertion.FieldValuesAssertion;
import com.linkedin.assertion.FieldValuesFailThreshold;
import com.linkedin.assertion.FieldValuesFailThresholdType;
import com.linkedin.assertion.FreshnessFieldKind;
import com.linkedin.assertion.FreshnessFieldSpec;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.metadata.service.util.MonitorServiceUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFieldAssertionSourceType;
import com.linkedin.schema.SchemaFieldSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.testng.Assert;

public class MonitorServiceUtilsTest {
    @Test
    public void testBuildAssertionStdParametersJson() {
        AssertionStdParameters stdParameters = new AssertionStdParameters()
                .setValue(new AssertionStdParameter()
                        .setType(AssertionStdParameterType.STRING)
                        .setValue("TEST"))
                .setMinValue(new AssertionStdParameter()
                        .setType(AssertionStdParameterType.NUMBER)
                        .setValue("10"))
                .setMaxValue(new AssertionStdParameter()
                        .setType(AssertionStdParameterType.NUMBER)
                        .setValue("15"));
        ObjectNode objectNode = MonitorServiceUtils.buildAssertionStdParametersJson(stdParameters);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("TEST", objectNode.get("value").get("value").asText());
        Assert.assertEquals("STRING", objectNode.get("value").get("type").asText());
        Assert.assertEquals("10", objectNode.get("minValue").get("value").asText());
        Assert.assertEquals("NUMBER", objectNode.get("minValue").get("type").asText());
        Assert.assertEquals("15", objectNode.get("maxValue").get("value").asText());
        Assert.assertEquals("NUMBER", objectNode.get("maxValue").get("type").asText());
    }

    @Test
    public void testBuildSchemaFieldSpecJson() {
        SchemaFieldSpec schemaFieldSpec = new SchemaFieldSpec()
                .setPath("path")
                .setType("type")
                .setNativeType("nativeType");

        ObjectNode objectNode = MonitorServiceUtils.buildSchemaFieldSpecJson(schemaFieldSpec);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("path", objectNode.get("path").asText());
        Assert.assertEquals("type", objectNode.get("type").asText());
        Assert.assertEquals("nativeType", objectNode.get("nativeType").asText());
    }

    @Test
    public void testBuildFreshnessFieldSpecJson() {
        FreshnessFieldSpec freshnessFieldSpec = new FreshnessFieldSpec()
                .setPath("path")
                .setType("type")
                .setNativeType("nativeType")
                .setKind(FreshnessFieldKind.HIGH_WATERMARK);
        ObjectNode objectNode = MonitorServiceUtils.buildFreshnessFieldSpecJson(freshnessFieldSpec);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("path", objectNode.get("path").asText());
        Assert.assertEquals("type", objectNode.get("type").asText());
        Assert.assertEquals("nativeType", objectNode.get("nativeType").asText());
        Assert.assertEquals("HIGH_WATERMARK", objectNode.get("kind").asText());
    }

    @Test
    public void testBuildFieldValuesAssertionJson() {
        FieldValuesAssertion fieldValuesAssertion = new FieldValuesAssertion()
                .setOperator(AssertionStdOperator.IS_TRUE)
                .setExcludeNulls(true)
                .setField(new SchemaFieldSpec()
                        .setPath("path")
                        .setType("type")
                        .setNativeType("nativeType"))
                .setFailThreshold(new FieldValuesFailThreshold()
                        .setType(FieldValuesFailThresholdType.COUNT)
                        .setValue(10))
                .setTransform(new FieldTransform()
                        .setType(FieldTransformType.LENGTH));
        ObjectNode objectNode = MonitorServiceUtils.buildFieldValuesAssertionJson(fieldValuesAssertion);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("IS_TRUE", objectNode.get("operator").asText());
        Assert.assertEquals(true, objectNode.get("excludeNulls").asBoolean());
        Assert.assertEquals("path", objectNode.get("field").get("path").asText());
        Assert.assertEquals("type", objectNode.get("field").get("type").asText());
        Assert.assertEquals("nativeType", objectNode.get("field").get("nativeType").asText());
        Assert.assertEquals("COUNT", objectNode.get("failThreshold").get("type").asText());
        Assert.assertEquals(10, objectNode.get("failThreshold").get("value").asInt());
        Assert.assertEquals("LENGTH", objectNode.get("transform").get("type").asText());
    }

    @Test
    public void testBuildFieldMetricAssertionJson() {
        FieldMetricAssertion fieldMetricAssertion = new FieldMetricAssertion()
                .setOperator(AssertionStdOperator.IS_TRUE)
                .setMetric(FieldMetricType.MEAN)
                .setField(new SchemaFieldSpec()
                        .setPath("path")
                        .setType("type")
                        .setNativeType("nativeType"))
                .setParameters(new AssertionStdParameters()
                        .setValue(new AssertionStdParameter()
                                .setType(AssertionStdParameterType.NUMBER)
                                .setValue("3.14")));
        ObjectNode objectNode = MonitorServiceUtils.buildFieldMetricAssertionJson(fieldMetricAssertion);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("IS_TRUE", objectNode.get("operator").asText());
        Assert.assertEquals("MEAN", objectNode.get("metric").asText());
        Assert.assertEquals("path", objectNode.get("field").get("path").asText());
        Assert.assertEquals("type", objectNode.get("field").get("type").asText());
        Assert.assertEquals("nativeType", objectNode.get("field").get("nativeType").asText());
        Assert.assertEquals("NUMBER", objectNode.get("parameters").get("value").get("type").asText());
        Assert.assertEquals("3.14", objectNode.get("parameters").get("value").get("value").asText());
    }

    @Test
    public void testBuildDatasetFieldParametersJson() {
        DatasetFieldAssertionParameters fieldAssertionParameters = new DatasetFieldAssertionParameters()
                .setSourceType(DatasetFieldAssertionSourceType.CHANGED_ROWS_QUERY)
                .setChangedRowsField(new FreshnessFieldSpec()
                        .setPath("path")
                        .setType("type")
                        .setNativeType("nativeType")
                        .setKind(FreshnessFieldKind.HIGH_WATERMARK));
        ObjectNode objectNode = MonitorServiceUtils.buildDatasetFieldParametersJson(fieldAssertionParameters);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("CHANGED_ROWS_QUERY", objectNode.get("sourceType").asText());
        Assert.assertEquals("path", objectNode.get("changedRowsField").get("path").asText());
        Assert.assertEquals("type", objectNode.get("changedRowsField").get("type").asText());
        Assert.assertEquals("nativeType", objectNode.get("changedRowsField").get("nativeType").asText());
        Assert.assertEquals("HIGH_WATERMARK", objectNode.get("changedRowsField").get("kind").asText());
    }

    @Test
    public void testBuildAssertionEvaluationParametersJson() {
        AssertionEvaluationParameters assertionParameters = new AssertionEvaluationParameters()
                .setType(AssertionEvaluationParametersType.DATASET_FIELD)
                .setDatasetFieldParameters(new DatasetFieldAssertionParameters()
                        .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY));
        ObjectNode objectNode = MonitorServiceUtils.buildAssertionEvaluationParametersJson(assertionParameters);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("DATASET_FIELD", objectNode.get("type").asText());
        Assert.assertEquals("ALL_ROWS_QUERY", objectNode.get("datasetFieldParameters").get("sourceType").asText());
    }

    @Test
    public void testBuildTestSqlAssertionBodyJson() throws Exception {
        String type = "DATASET_SQL";
        String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
        String connectionUrn = "urn:li:dataPlatform:snowflake";
        SqlAssertionInfo sqlAssertionInfo = new SqlAssertionInfo()
                .setType(SqlAssertionType.METRIC)
                .setStatement("SELECT COUNT(*) FROM TEST")
                .setChangeType(AssertionValueChangeType.ABSOLUTE)
                .setOperator(AssertionStdOperator.GREATER_THAN)
                .setParameters(new AssertionStdParameters()
                        .setValue(new AssertionStdParameter()
                                .setType(AssertionStdParameterType.NUMBER)
                                .setValue("10")));
        String jsonString = MonitorServiceUtils.buildTestSqlAssertionBodyJson(type, asserteeUrn, connectionUrn,
                sqlAssertionInfo);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("DATASET_SQL", objectNode.get("type").asText());
        Assert.assertEquals(asserteeUrn,
                objectNode.get("entityUrn").asText());
        Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
        Assert.assertEquals("METRIC",
                objectNode.get("assertion").get("sqlAssertion").get("type").asText());
        Assert.assertEquals("SELECT COUNT(*) FROM TEST",
                objectNode.get("assertion").get("sqlAssertion").get("statement").asText());
        Assert.assertEquals("ABSOLUTE",
                objectNode.get("assertion").get("sqlAssertion").get("changeType").asText());
        Assert.assertEquals("GREATER_THAN",
                objectNode.get("assertion").get("sqlAssertion").get("operator").asText());
        Assert.assertEquals("NUMBER",
                objectNode.get("assertion").get("sqlAssertion").get("parameters").get("value").get("type").asText());
        Assert.assertEquals("10",
                objectNode.get("assertion").get("sqlAssertion").get("parameters").get("value").get("value").asText());
    }

    @Test
    public void testBuildTestFieldAssertionBodyJson() throws Exception {
        String type = "DATASET_FIELD";
        String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
        String connectionUrn = "urn:li:dataPlatform:snowflake";
        FieldAssertionInfo fieldAssertionInfo = new FieldAssertionInfo()
                .setType(FieldAssertionType.FIELD_VALUES)
                .setFilter(new DatasetFilter()
                        .setSql("WHERE value IS NOT NULL")
                        .setType(DatasetFilterType.SQL))
                .setFieldValuesAssertion(new FieldValuesAssertion()
                        .setOperator(AssertionStdOperator.IS_TRUE)
                        .setExcludeNulls(true)
                        .setField(new SchemaFieldSpec()
                                .setPath("path")
                                .setType("type")
                                .setNativeType("nativeType"))
                        .setFailThreshold(new FieldValuesFailThreshold()
                                .setType(FieldValuesFailThresholdType.COUNT)
                                .setValue(10))
                        .setTransform(new FieldTransform()
                                .setType(FieldTransformType.LENGTH)));
        AssertionEvaluationParameters assertionParameters = new AssertionEvaluationParameters()
                .setType(AssertionEvaluationParametersType.DATASET_FIELD)
                .setDatasetFieldParameters(new DatasetFieldAssertionParameters()
                        .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY));
        String jsonString = MonitorServiceUtils.buildTestFieldAssertionBodyJson(type, asserteeUrn, connectionUrn,
                fieldAssertionInfo, assertionParameters);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

        Assert.assertNotNull(objectNode);
        Assert.assertEquals("DATASET_FIELD", objectNode.get("type").asText());
        Assert.assertEquals(asserteeUrn,
                objectNode.get("entityUrn").asText());
        Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
        Assert.assertEquals("WHERE value IS NOT NULL",
                objectNode.get("assertion").get("fieldAssertion").get("filter").get("sql").asText());
        Assert.assertEquals("SQL",
                objectNode.get("assertion").get("fieldAssertion").get("filter").get("type").asText());
        Assert.assertEquals("IS_TRUE",
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("operator").asText());
        Assert.assertEquals(true,
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("excludeNulls").asBoolean());
        Assert.assertEquals("path",
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("field").get("path").asText());
        Assert.assertEquals("type",
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("field").get("type").asText());
        Assert.assertEquals("nativeType",
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("field").get("nativeType").asText());
        Assert.assertEquals("COUNT",
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("failThreshold").get("type").asText());
        Assert.assertEquals(10,
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("failThreshold").get("value").asInt());
        Assert.assertEquals("LENGTH",
                objectNode.get("assertion").get("fieldAssertion").get("fieldValuesAssertion")
                        .get("transform").get("type").asText());
        Assert.assertEquals("DATASET_FIELD", objectNode.get("parameters").get("type").asText());
        Assert.assertEquals("ALL_ROWS_QUERY",
                objectNode.get("parameters").get("datasetFieldParameters").get("sourceType").asText());
    }

    @Test
    public void testBuildTestAssertionResult() {
        try {
            String jsonStr = "{\"type\":\"SUCCESS\",\"rowCount\":10,\"missingCount\":0,\"unexpectedCount\":0,"
                        + "\"actualAggValue\":null,\"externalUrl\":null,\"nativeResults\":null,\"error\":null}";
            AssertionResult expectedAssertionResult = new AssertionResult()
                    .setType(AssertionResultType.SUCCESS)
                    .setRowCount(10L)
                    .setMissingCount(0L)
                    .setUnexpectedCount(0L);
            AssertionResult assertionResult = MonitorServiceUtils.buildTestAssertionResult(jsonStr);

            Assert.assertNotNull(assertionResult);
            Assert.assertEquals(expectedAssertionResult.getType(), assertionResult.getType());
            Assert.assertEquals(expectedAssertionResult.getRowCount(), assertionResult.getRowCount());
            Assert.assertEquals(expectedAssertionResult.getMissingCount(), assertionResult.getMissingCount());
            Assert.assertEquals(expectedAssertionResult.getUnexpectedCount(), assertionResult.getUnexpectedCount());
        } catch (Exception e) {
            Assert.fail("Exception should not be thrown here.");
        }
    }
}
