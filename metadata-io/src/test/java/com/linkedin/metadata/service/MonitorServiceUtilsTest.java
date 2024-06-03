package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
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
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.assertion.FreshnessFieldKind;
import com.linkedin.assertion.FreshnessFieldSpec;
import com.linkedin.assertion.IncrementingSegmentFieldTransformer;
import com.linkedin.assertion.IncrementingSegmentFieldTransformerType;
import com.linkedin.assertion.IncrementingSegmentRowCountChange;
import com.linkedin.assertion.IncrementingSegmentRowCountTotal;
import com.linkedin.assertion.IncrementingSegmentSpec;
import com.linkedin.assertion.RowCountChange;
import com.linkedin.assertion.RowCountTotal;
import com.linkedin.assertion.SchemaAssertionCompatibility;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.metadata.service.util.MonitorServiceUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DataHubOperationSpec;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFieldAssertionSourceType;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.monitor.DatasetSchemaAssertionParameters;
import com.linkedin.monitor.DatasetSchemaSourceType;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaFieldSpec;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.timeseries.CalendarInterval;
import java.util.Collections;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorServiceUtilsTest {
  @Test
  public void testBuildAssertionStdParametersJson() {
    AssertionStdParameters stdParameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.STRING)
                    .setValue("TEST"))
            .setMinValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("10"))
            .setMaxValue(
                new AssertionStdParameter()
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
  public void testBuildAuditLogSpecJson() {
    AuditLogSpec auditLogSpec =
        new AuditLogSpec()
            .setOperationTypes(new StringArray(ImmutableList.of("INSERT", "DELETE")))
            .setUserName("testUser");

    ObjectNode objectNode = MonitorServiceUtils.buildAuditLogSpecJson(auditLogSpec);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("[INSERT, DELETE]", objectNode.get("operationTypes").asText());
    Assert.assertEquals("testUser", objectNode.get("userName").asText());
  }

  @Test
  public void testBuildDataHubOperationSpecJson() {
    DataHubOperationSpec datahubOperationSpec =
        new DataHubOperationSpec()
            .setOperationTypes(new StringArray(ImmutableList.of("INSERT", "DELETE")))
            .setCustomOperationTypes(new StringArray(ImmutableList.of("SUM", "AVG")));

    ObjectNode objectNode = MonitorServiceUtils.buildDataHubOperationJson(datahubOperationSpec);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("[INSERT, DELETE]", objectNode.get("operationTypes").asText());
    Assert.assertEquals("[SUM, AVG]", objectNode.get("customOperationTypes").asText());
  }

  @Test
  public void testBuildSchemaFieldSpecJson() {
    SchemaFieldSpec schemaFieldSpec =
        new SchemaFieldSpec().setPath("path").setType("type").setNativeType("nativeType");

    ObjectNode objectNode = MonitorServiceUtils.buildSchemaFieldSpecJson(schemaFieldSpec);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("path", objectNode.get("path").asText());
    Assert.assertEquals("type", objectNode.get("type").asText());
    Assert.assertEquals("nativeType", objectNode.get("nativeType").asText());
  }

  @Test
  public void testBuildFreshnessFieldSpecJson() {
    FreshnessFieldSpec freshnessFieldSpec =
        new FreshnessFieldSpec()
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
  public void testBuildFreshnessCronScheduleJson() {
    FreshnessCronSchedule freshnessCronSchedule =
        new FreshnessCronSchedule()
            .setCron("cron")
            .setTimezone("timezone")
            .setWindowStartOffsetMs(1234567890);
    ObjectNode objectNode =
        MonitorServiceUtils.buildFreshnessCronScheduleJson(freshnessCronSchedule);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("cron", objectNode.get("cron").asText());
    Assert.assertEquals("timezone", objectNode.get("timezone").asText());
    Assert.assertEquals("1234567890", objectNode.get("windowStartOffsetMs").asText());
  }

  @Test
  public void testBuildFixedIntervalScheduleJson() {
    FixedIntervalSchedule freshnessCronSchedule =
        new FixedIntervalSchedule().setUnit(CalendarInterval.HOUR).setMultiple(1);
    ObjectNode objectNode =
        MonitorServiceUtils.buildFixedIntervalScheduleJson(freshnessCronSchedule);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("HOUR", objectNode.get("unit").asText());
    Assert.assertEquals("1", objectNode.get("multiple").asText());
  }

  @Test
  public void testBuildFieldValuesAssertionJson() {
    FieldValuesAssertion fieldValuesAssertion =
        new FieldValuesAssertion()
            .setOperator(AssertionStdOperator.IS_TRUE)
            .setExcludeNulls(true)
            .setField(
                new SchemaFieldSpec().setPath("path").setType("type").setNativeType("nativeType"))
            .setFailThreshold(
                new FieldValuesFailThreshold()
                    .setType(FieldValuesFailThresholdType.COUNT)
                    .setValue(10))
            .setTransform(new FieldTransform().setType(FieldTransformType.LENGTH));
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
  public void testBuildFreshnessAssertionScheduleJson() {
    FreshnessAssertionSchedule freshnessCronSchedule =
        new FreshnessAssertionSchedule()
            .setType(FreshnessAssertionScheduleType.CRON)
            .setCron(
                new FreshnessCronSchedule()
                    .setCron("cron")
                    .setTimezone("timezone")
                    .setWindowStartOffsetMs(1234567890));
    ObjectNode objectNode =
        MonitorServiceUtils.buildFreshnessAssertionScheduleJson(freshnessCronSchedule);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("CRON", objectNode.get("type").asText());
    Assert.assertEquals("cron", objectNode.get("cron").get("cron").asText());
    Assert.assertEquals("timezone", objectNode.get("cron").get("timezone").asText());
    Assert.assertEquals("1234567890", objectNode.get("cron").get("windowStartOffsetMs").asText());
  }

  @Test
  public void testBuildIncrementingSegmentSpecJson() {
    IncrementingSegmentSpec freshnessCronSchedule =
        new IncrementingSegmentSpec()
            .setField(
                new SchemaFieldSpec().setPath("path").setType("type").setNativeType("nativeType"))
            .setTransformer(
                new IncrementingSegmentFieldTransformer()
                    .setType(IncrementingSegmentFieldTransformerType.TIMESTAMP_MS_TO_MINUTE)
                    .setNativeType("nativeType"));
    ObjectNode objectNode =
        MonitorServiceUtils.buildIncrementingSegmentSpecJson(freshnessCronSchedule);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("path", objectNode.get("field").get("path").asText());
    Assert.assertEquals("type", objectNode.get("field").get("type").asText());
    Assert.assertEquals("nativeType", objectNode.get("field").get("nativeType").asText());
    Assert.assertEquals(
        "TIMESTAMP_MS_TO_MINUTE", objectNode.get("transformer").get("type").asText());
    Assert.assertEquals("nativeType", objectNode.get("transformer").get("nativeType").asText());
  }

  @Test
  public void testBuildFieldMetricAssertionJson() {
    FieldMetricAssertion fieldMetricAssertion =
        new FieldMetricAssertion()
            .setOperator(AssertionStdOperator.IS_TRUE)
            .setMetric(FieldMetricType.MEAN)
            .setField(
                new SchemaFieldSpec().setPath("path").setType("type").setNativeType("nativeType"))
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
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
  public void testBuildDatasetFreshnessAssertionParametersJson() {
    DatasetFreshnessAssertionParameters fieldAssertionParameters =
        new DatasetFreshnessAssertionParameters()
            .setSourceType(DatasetFreshnessSourceType.FIELD_VALUE)
            .setField(
                new FreshnessFieldSpec()
                    .setPath("path")
                    .setType("type")
                    .setNativeType("nativeType")
                    .setKind(FreshnessFieldKind.HIGH_WATERMARK));
    ObjectNode objectNode =
        MonitorServiceUtils.buildDatasetFreshnessParametersJson(fieldAssertionParameters);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("FIELD_VALUE", objectNode.get("sourceType").asText());
    Assert.assertEquals("path", objectNode.get("field").get("path").asText());
    Assert.assertEquals("type", objectNode.get("field").get("type").asText());
    Assert.assertEquals("nativeType", objectNode.get("field").get("nativeType").asText());
    Assert.assertEquals("HIGH_WATERMARK", objectNode.get("field").get("kind").asText());
  }

  @Test
  public void testBuildDatasetFieldParametersJson() {
    DatasetFieldAssertionParameters fieldAssertionParameters =
        new DatasetFieldAssertionParameters()
            .setSourceType(DatasetFieldAssertionSourceType.CHANGED_ROWS_QUERY)
            .setChangedRowsField(
                new FreshnessFieldSpec()
                    .setPath("path")
                    .setType("type")
                    .setNativeType("nativeType")
                    .setKind(FreshnessFieldKind.HIGH_WATERMARK));
    ObjectNode objectNode =
        MonitorServiceUtils.buildDatasetFieldParametersJson(fieldAssertionParameters);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("CHANGED_ROWS_QUERY", objectNode.get("sourceType").asText());
    Assert.assertEquals("path", objectNode.get("changedRowsField").get("path").asText());
    Assert.assertEquals("type", objectNode.get("changedRowsField").get("type").asText());
    Assert.assertEquals(
        "nativeType", objectNode.get("changedRowsField").get("nativeType").asText());
    Assert.assertEquals("HIGH_WATERMARK", objectNode.get("changedRowsField").get("kind").asText());
  }

  @Test
  public void testBuildAssertionEvaluationParametersJson() {
    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FIELD)
            .setDatasetFieldParameters(
                new DatasetFieldAssertionParameters()
                    .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY));
    ObjectNode objectNode =
        MonitorServiceUtils.buildAssertionEvaluationParametersJson(assertionParameters);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_FIELD", objectNode.get("type").asText());
    Assert.assertEquals(
        "ALL_ROWS_QUERY", objectNode.get("datasetFieldParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestFreshnessAssertionBodyJson() throws Exception {
    String type = "DATASET_FRESSHNESS";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    FreshnessAssertionInfo freshnessAssertionInfo =
        new FreshnessAssertionInfo()
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setSchedule(
                new FreshnessAssertionSchedule()
                    .setType(FreshnessAssertionScheduleType.CRON)
                    .setCron(
                        new FreshnessCronSchedule()
                            .setCron("cron")
                            .setTimezone("timezone")
                            .setWindowStartOffsetMs(1234567890)))
            .setFilter(
                new DatasetFilter()
                    .setSql("WHERE value IS NOT NULL")
                    .setType(DatasetFilterType.SQL));

    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS)
            .setDatasetFreshnessParameters(
                new DatasetFreshnessAssertionParameters()
                    .setSourceType(DatasetFreshnessSourceType.AUDIT_LOG));

    String jsonString =
        MonitorServiceUtils.buildTestFreshnessAssertionBodyJson(
            type, asserteeUrn, connectionUrn, freshnessAssertionInfo, assertionParameters);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_FRESSHNESS", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "CRON",
        objectNode.get("assertion").get("freshnessAssertion").get("schedule").get("type").asText());
    Assert.assertEquals(
        "cron",
        objectNode
            .get("assertion")
            .get("freshnessAssertion")
            .get("schedule")
            .get("cron")
            .get("cron")
            .asText());
    Assert.assertEquals(
        "timezone",
        objectNode
            .get("assertion")
            .get("freshnessAssertion")
            .get("schedule")
            .get("cron")
            .get("timezone")
            .asText());
    Assert.assertEquals(
        "1234567890",
        objectNode
            .get("assertion")
            .get("freshnessAssertion")
            .get("schedule")
            .get("cron")
            .get("windowStartOffsetMs")
            .asText());

    Assert.assertEquals(
        "WHERE value IS NOT NULL",
        objectNode.get("assertion").get("freshnessAssertion").get("filter").get("sql").asText());
    Assert.assertEquals(
        "SQL",
        objectNode.get("assertion").get("freshnessAssertion").get("filter").get("type").asText());
    Assert.assertEquals("DATASET_FRESHNESS", objectNode.get("parameters").get("type").asText());
    Assert.assertEquals(
        "AUDIT_LOG",
        objectNode.get("parameters").get("datasetFreshnessParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestVolumeAssertionBodyJsonRowCountTotal() throws Exception {
    String type = "DATASET_VOLUME";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setRowCountTotal(
                new RowCountTotal()
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10"))))
            .setFilter(
                new DatasetFilter()
                    .setSql("WHERE value IS NOT NULL")
                    .setType(DatasetFilterType.SQL));

    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(
                new DatasetVolumeAssertionParameters()
                    .setSourceType(DatasetVolumeSourceType.INFORMATION_SCHEMA));

    String jsonString =
        MonitorServiceUtils.buildTestVolumeAssertionBodyJson(
            type, asserteeUrn, connectionUrn, volumeAssertionInfo, assertionParameters);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "EQUAL_TO",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountTotal")
            .get("operator")
            .asText());
    Assert.assertEquals(
        "NUMBER",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountTotal")
            .get("parameters")
            .get("value")
            .get("type")
            .asText());
    Assert.assertEquals(
        "10",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountTotal")
            .get("parameters")
            .get("value")
            .get("value")
            .asText());
    Assert.assertEquals(
        "WHERE value IS NOT NULL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("sql").asText());
    Assert.assertEquals(
        "SQL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("type").asText());
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("parameters").get("type").asText());
    Assert.assertEquals(
        "INFORMATION_SCHEMA",
        objectNode.get("parameters").get("datasetVolumeParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestVolumeAssertionBodyJsonRowCountChange() throws Exception {
    String type = "DATASET_VOLUME";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setType(VolumeAssertionType.ROW_COUNT_CHANGE)
            .setRowCountChange(
                new RowCountChange()
                    .setType(AssertionValueChangeType.ABSOLUTE)
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10"))))
            .setFilter(
                new DatasetFilter()
                    .setSql("WHERE value IS NOT NULL")
                    .setType(DatasetFilterType.SQL));

    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(
                new DatasetVolumeAssertionParameters()
                    .setSourceType(DatasetVolumeSourceType.INFORMATION_SCHEMA));

    String jsonString =
        MonitorServiceUtils.buildTestVolumeAssertionBodyJson(
            type, asserteeUrn, connectionUrn, volumeAssertionInfo, assertionParameters);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "ABSOLUTE",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountChange")
            .get("type")
            .asText());
    Assert.assertEquals(
        "EQUAL_TO",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountChange")
            .get("operator")
            .asText());
    Assert.assertEquals(
        "NUMBER",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountChange")
            .get("parameters")
            .get("value")
            .get("type")
            .asText());
    Assert.assertEquals(
        "10",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("rowCountChange")
            .get("parameters")
            .get("value")
            .get("value")
            .asText());
    Assert.assertEquals(
        "WHERE value IS NOT NULL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("sql").asText());
    Assert.assertEquals(
        "SQL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("type").asText());
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("parameters").get("type").asText());
    Assert.assertEquals(
        "INFORMATION_SCHEMA",
        objectNode.get("parameters").get("datasetVolumeParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestVolumeAssertionBodyJsonIncrementingSegmentRowCountTotal()
      throws Exception {
    String type = "DATASET_VOLUME";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setIncrementingSegmentRowCountTotal(
                new IncrementingSegmentRowCountTotal()
                    .setSegment(
                        new IncrementingSegmentSpec()
                            .setField(
                                new SchemaFieldSpec()
                                    .setPath("path")
                                    .setType("type")
                                    .setNativeType("nativeType"))
                            .setTransformer(
                                new IncrementingSegmentFieldTransformer()
                                    .setType(
                                        IncrementingSegmentFieldTransformerType
                                            .TIMESTAMP_MS_TO_HOUR)
                                    .setNativeType("nativeType")))
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10"))))
            .setFilter(
                new DatasetFilter()
                    .setSql("WHERE value IS NOT NULL")
                    .setType(DatasetFilterType.SQL));

    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(
                new DatasetVolumeAssertionParameters()
                    .setSourceType(DatasetVolumeSourceType.INFORMATION_SCHEMA));

    String jsonString =
        MonitorServiceUtils.buildTestVolumeAssertionBodyJson(
            type, asserteeUrn, connectionUrn, volumeAssertionInfo, assertionParameters);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "path",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("segment")
            .get("field")
            .get("path")
            .asText());
    Assert.assertEquals(
        "type",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("segment")
            .get("field")
            .get("type")
            .asText());
    Assert.assertEquals(
        "nativeType",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("segment")
            .get("field")
            .get("nativeType")
            .asText());
    Assert.assertEquals(
        "TIMESTAMP_MS_TO_HOUR",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("segment")
            .get("transformer")
            .get("type")
            .asText());
    Assert.assertEquals(
        "nativeType",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("segment")
            .get("transformer")
            .get("nativeType")
            .asText());
    Assert.assertEquals(
        "EQUAL_TO",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("operator")
            .asText());
    Assert.assertEquals(
        "NUMBER",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("parameters")
            .get("value")
            .get("type")
            .asText());
    Assert.assertEquals(
        "10",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountTotal")
            .get("parameters")
            .get("value")
            .get("value")
            .asText());
    Assert.assertEquals(
        "WHERE value IS NOT NULL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("sql").asText());
    Assert.assertEquals(
        "SQL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("type").asText());
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("parameters").get("type").asText());
    Assert.assertEquals(
        "INFORMATION_SCHEMA",
        objectNode.get("parameters").get("datasetVolumeParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestVolumeAssertionBodyJsonIncrementingSegmentRowCountChange()
      throws Exception {
    String type = "DATASET_VOLUME";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setType(VolumeAssertionType.ROW_COUNT_CHANGE)
            .setIncrementingSegmentRowCountChange(
                new IncrementingSegmentRowCountChange()
                    .setSegment(
                        new IncrementingSegmentSpec()
                            .setField(
                                new SchemaFieldSpec()
                                    .setPath("path")
                                    .setType("type")
                                    .setNativeType("nativeType"))
                            .setTransformer(
                                new IncrementingSegmentFieldTransformer()
                                    .setType(
                                        IncrementingSegmentFieldTransformerType
                                            .TIMESTAMP_MS_TO_HOUR)
                                    .setNativeType("nativeType")))
                    .setType(AssertionValueChangeType.ABSOLUTE)
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10"))))
            .setFilter(
                new DatasetFilter()
                    .setSql("WHERE value IS NOT NULL")
                    .setType(DatasetFilterType.SQL));

    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(
                new DatasetVolumeAssertionParameters()
                    .setSourceType(DatasetVolumeSourceType.INFORMATION_SCHEMA));

    String jsonString =
        MonitorServiceUtils.buildTestVolumeAssertionBodyJson(
            type, asserteeUrn, connectionUrn, volumeAssertionInfo, assertionParameters);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "path",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("segment")
            .get("field")
            .get("path")
            .asText());
    Assert.assertEquals(
        "type",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("segment")
            .get("field")
            .get("type")
            .asText());
    Assert.assertEquals(
        "nativeType",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("segment")
            .get("field")
            .get("nativeType")
            .asText());
    Assert.assertEquals(
        "TIMESTAMP_MS_TO_HOUR",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("segment")
            .get("transformer")
            .get("type")
            .asText());
    Assert.assertEquals(
        "nativeType",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("segment")
            .get("transformer")
            .get("nativeType")
            .asText());
    Assert.assertEquals(
        "ABSOLUTE",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("type")
            .asText());
    Assert.assertEquals(
        "EQUAL_TO",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("operator")
            .asText());
    Assert.assertEquals(
        "NUMBER",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("parameters")
            .get("value")
            .get("type")
            .asText());
    Assert.assertEquals(
        "10",
        objectNode
            .get("assertion")
            .get("volumeAssertion")
            .get("incrementingSegmentRowCountChange")
            .get("parameters")
            .get("value")
            .get("value")
            .asText());
    Assert.assertEquals(
        "WHERE value IS NOT NULL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("sql").asText());
    Assert.assertEquals(
        "SQL",
        objectNode.get("assertion").get("volumeAssertion").get("filter").get("type").asText());
    Assert.assertEquals("DATASET_VOLUME", objectNode.get("parameters").get("type").asText());
    Assert.assertEquals(
        "INFORMATION_SCHEMA",
        objectNode.get("parameters").get("datasetVolumeParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestSqlAssertionBodyJson() throws Exception {
    String type = "DATASET_SQL";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    SqlAssertionInfo sqlAssertionInfo =
        new SqlAssertionInfo()
            .setType(SqlAssertionType.METRIC)
            .setStatement("SELECT COUNT(*) FROM TEST")
            .setChangeType(AssertionValueChangeType.ABSOLUTE)
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setType(AssertionStdParameterType.NUMBER)
                            .setValue("10")));
    String jsonString =
        MonitorServiceUtils.buildTestSqlAssertionBodyJson(
            type, asserteeUrn, connectionUrn, sqlAssertionInfo);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_SQL", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "METRIC", objectNode.get("assertion").get("sqlAssertion").get("type").asText());
    Assert.assertEquals(
        "SELECT COUNT(*) FROM TEST",
        objectNode.get("assertion").get("sqlAssertion").get("statement").asText());
    Assert.assertEquals(
        "ABSOLUTE", objectNode.get("assertion").get("sqlAssertion").get("changeType").asText());
    Assert.assertEquals(
        "GREATER_THAN", objectNode.get("assertion").get("sqlAssertion").get("operator").asText());
    Assert.assertEquals(
        "NUMBER",
        objectNode
            .get("assertion")
            .get("sqlAssertion")
            .get("parameters")
            .get("value")
            .get("type")
            .asText());
    Assert.assertEquals(
        "10",
        objectNode
            .get("assertion")
            .get("sqlAssertion")
            .get("parameters")
            .get("value")
            .get("value")
            .asText());
  }

  @Test
  public void testBuildTestFieldAssertionBodyJson() throws Exception {
    String type = "DATASET_FIELD";
    String asserteeUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)";
    String connectionUrn = "urn:li:dataPlatform:snowflake";
    FieldAssertionInfo fieldAssertionInfo =
        new FieldAssertionInfo()
            .setType(FieldAssertionType.FIELD_VALUES)
            .setFilter(
                new DatasetFilter()
                    .setSql("WHERE value IS NOT NULL")
                    .setType(DatasetFilterType.SQL))
            .setFieldValuesAssertion(
                new FieldValuesAssertion()
                    .setOperator(AssertionStdOperator.IS_TRUE)
                    .setExcludeNulls(true)
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("path")
                            .setType("type")
                            .setNativeType("nativeType"))
                    .setFailThreshold(
                        new FieldValuesFailThreshold()
                            .setType(FieldValuesFailThresholdType.COUNT)
                            .setValue(10))
                    .setTransform(new FieldTransform().setType(FieldTransformType.LENGTH)));
    AssertionEvaluationParameters assertionParameters =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FIELD)
            .setDatasetFieldParameters(
                new DatasetFieldAssertionParameters()
                    .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY));
    String jsonString =
        MonitorServiceUtils.buildTestFieldAssertionBodyJson(
            type, asserteeUrn, connectionUrn, fieldAssertionInfo, assertionParameters);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = (ObjectNode) mapper.readTree(jsonString);

    Assert.assertNotNull(objectNode);
    Assert.assertEquals("DATASET_FIELD", objectNode.get("type").asText());
    Assert.assertEquals(asserteeUrn, objectNode.get("entityUrn").asText());
    Assert.assertEquals(connectionUrn, objectNode.get("connectionUrn").asText());
    Assert.assertEquals(
        "WHERE value IS NOT NULL",
        objectNode.get("assertion").get("fieldAssertion").get("filter").get("sql").asText());
    Assert.assertEquals(
        "SQL",
        objectNode.get("assertion").get("fieldAssertion").get("filter").get("type").asText());
    Assert.assertEquals(
        "IS_TRUE",
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("operator")
            .asText());
    Assert.assertEquals(
        true,
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("excludeNulls")
            .asBoolean());
    Assert.assertEquals(
        "path",
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("field")
            .get("path")
            .asText());
    Assert.assertEquals(
        "type",
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("field")
            .get("type")
            .asText());
    Assert.assertEquals(
        "nativeType",
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("field")
            .get("nativeType")
            .asText());
    Assert.assertEquals(
        "COUNT",
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("failThreshold")
            .get("type")
            .asText());
    Assert.assertEquals(
        10,
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("failThreshold")
            .get("value")
            .asInt());
    Assert.assertEquals(
        "LENGTH",
        objectNode
            .get("assertion")
            .get("fieldAssertion")
            .get("fieldValuesAssertion")
            .get("transform")
            .get("type")
            .asText());
    Assert.assertEquals("DATASET_FIELD", objectNode.get("parameters").get("type").asText());
    Assert.assertEquals(
        "ALL_ROWS_QUERY",
        objectNode.get("parameters").get("datasetFieldParameters").get("sourceType").asText());
  }

  @Test
  public void testBuildTestAssertionResult() {
    try {
      String jsonStr =
          "{\"type\":\"SUCCESS\",\"rowCount\":10,\"missingCount\":0,\"unexpectedCount\":0,"
              + "\"actualAggValue\":null,\"externalUrl\":null,\"nativeResults\":null,\"error\":null}";
      AssertionResult expectedAssertionResult =
          new AssertionResult()
              .setType(AssertionResultType.SUCCESS)
              .setRowCount(10L)
              .setMissingCount(0L)
              .setUnexpectedCount(0L);
      AssertionResult assertionResult = MonitorServiceUtils.buildTestAssertionResult(jsonStr);

      Assert.assertNotNull(assertionResult);
      Assert.assertEquals(expectedAssertionResult.getType(), assertionResult.getType());
      Assert.assertEquals(expectedAssertionResult.getRowCount(), assertionResult.getRowCount());
      Assert.assertEquals(
          expectedAssertionResult.getMissingCount(), assertionResult.getMissingCount());
      Assert.assertEquals(
          expectedAssertionResult.getUnexpectedCount(), assertionResult.getUnexpectedCount());
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown here.");
    }
  }

  @Test
  public void testBuildDatasetSchemaParametersJson() {
    DatasetSchemaAssertionParameters parameters = new DatasetSchemaAssertionParameters();
    parameters.setSourceType(DatasetSchemaSourceType.DATAHUB_SCHEMA);
    ObjectNode objectNode = MonitorServiceUtils.buildDatasetSchemaParametersJson(parameters);
    Assert.assertEquals("DATAHUB_SCHEMA", objectNode.get("sourceType").asText());
  }

  @Test
  public void testBuildTestSchemaAssertionBodyJson() throws Exception {
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setVersion(0);
    schemaMetadata.setHash("testHash");
    schemaMetadata.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new OtherSchema()));
    schemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setFieldPath("testPath")
                    .setType(
                        new SchemaFieldDataType()
                            .setType(SchemaFieldDataType.Type.create(new StringType())))
                    .setNativeDataType("varchar"))));

    String bodyJson =
        MonitorServiceUtils.buildTestSchemaAssertionBodyJson(
            "DATASET_SCHEMA",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)",
            "urn:li:dataPlatform:snowflake",
            new SchemaAssertionInfo()
                .setCompatibility(SchemaAssertionCompatibility.SUPERSET)
                .setSchema(schemaMetadata),
            new AssertionEvaluationParameters()
                .setType(AssertionEvaluationParametersType.DATASET_SCHEMA)
                .setDatasetSchemaParameters(
                    new DatasetSchemaAssertionParameters()
                        .setSourceType(DatasetSchemaSourceType.DATAHUB_SCHEMA)));
    Assert.assertEquals(
        bodyJson,
        "{\n"
            + "  \"type\" : \"DATASET_SCHEMA\",\n"
            + "  \"connectionUrn\" : \"urn:li:dataPlatform:snowflake\",\n"
            + "  \"entityUrn\" : \"urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST,PROD)\",\n"
            + "  \"assertion\" : {\n"
            + "    \"schemaAssertion\" : {\n"
            + "      \"compatibility\" : \"SUPERSET\",\n"
            + "      \"fields\" : [ {\n"
            + "        \"path\" : \"testPath\",\n"
            + "        \"type\" : \"STRING\",\n"
            + "        \"nativeType\" : \"varchar\"\n"
            + "      } ]\n"
            + "    }\n"
            + "  },\n"
            + "  \"parameters\" : {\n"
            + "    \"type\" : \"DATASET_SCHEMA\",\n"
            + "    \"datasetSchemaParameters\" : {\n"
            + "      \"sourceType\" : \"DATAHUB_SCHEMA\"\n"
            + "    }\n"
            + "  }\n"
            + "}");
  }

  @Test
  public void testBuildRunAssertionsBodyJson() {
    Urn testUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn testUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    try {
      String bodyJson =
          MonitorServiceUtils.buildRunAssertionsBodyJson(
              ImmutableList.of(testUrn1, testUrn2), true, Collections.emptyMap(), false);
      Assert.assertEquals(
          bodyJson,
          "{\n"
              + "  \"urns\" : [ \"urn:li:assertion:test\", \"urn:li:assertion:test2\" ],\n"
              + "  \"dryRun\" : true,\n"
              + "  \"async\" : false\n"
              + "}");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown here.");
    }
  }

  @Test
  public void testBuildSchemaAssertionFieldJson() {
    SchemaField field =
        new SchemaField()
            .setFieldPath("testPath")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("varchar");
    ObjectNode objectNode = MonitorServiceUtils.buildSchemaAssertionFieldJson(field);
    Assert.assertEquals("testPath", objectNode.get("path").asText());
    Assert.assertEquals("STRING", objectNode.get("type").asText());
    Assert.assertEquals("varchar", objectNode.get("nativeType").asText());
  }

  @Test
  public void testBuildRunAssertionsResult() {
    Urn testAssertionUrn = UrnUtils.getUrn("urn:li:assertion:test");
    ObjectNode resultsJson = JsonNodeFactory.instance.objectNode();
    ArrayNode resultsArr = JsonNodeFactory.instance.arrayNode();
    ObjectNode assertionNode = JsonNodeFactory.instance.objectNode();
    ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
    resultNode.put("type", "SUCCESS");
    assertionNode.set("urn", new TextNode(testAssertionUrn.toString()));
    assertionNode.set("result", resultNode);
    resultsArr.add(assertionNode);
    resultsJson.set("results", resultsArr);

    // Write JSON to string
    String resultsJsonString = resultsJson.toString();

    try {
      Map<Urn, AssertionResult> result =
          MonitorServiceUtils.buildRunAssertionsResult(
              ImmutableList.of(testAssertionUrn), resultsJsonString);
      Assert.assertEquals(1, result.size());
      Assert.assertTrue(result.containsKey(testAssertionUrn));
      Assert.assertEquals(AssertionResultType.SUCCESS, result.get(testAssertionUrn).getType());
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown here.");
    }
  }
}
