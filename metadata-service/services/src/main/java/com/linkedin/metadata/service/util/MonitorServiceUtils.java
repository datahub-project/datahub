package com.linkedin.metadata.service.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldValuesAssertion;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.assertion.FreshnessFieldSpec;
import com.linkedin.assertion.IncrementingSegmentSpec;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DataHubOperationSpec;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetSchemaAssertionParameters;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class MonitorServiceUtils {
  public static ObjectNode buildAssertionStdParametersJson(
      @Nonnull final AssertionStdParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    if (parameters.hasValue()) {
      final AssertionStdParameter value = parameters.getValue();
      final ObjectNode valueNode = objectMapper.createObjectNode();
      valueNode.put("type", value.getType().toString());
      valueNode.put("value", value.getValue());
      objectNode.put("value", valueNode);
    }
    if (parameters.hasMaxValue()) {
      final AssertionStdParameter maxValue = parameters.getMaxValue();
      final ObjectNode maxValueNode = objectMapper.createObjectNode();
      maxValueNode.put("type", maxValue.getType().toString());
      maxValueNode.put("value", maxValue.getValue());
      objectNode.put("maxValue", maxValueNode);
    }
    if (parameters.hasMinValue()) {
      final AssertionStdParameter minValue = parameters.getMinValue();
      final ObjectNode minValueNode = objectMapper.createObjectNode();
      minValueNode.put("type", minValue.getType().toString());
      minValueNode.put("value", minValue.getValue());
      objectNode.put("minValue", minValueNode);
    }

    return objectNode;
  }

  public static ObjectNode buildAuditLogSpecJson(@Nonnull final AuditLogSpec field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    if (field.hasOperationTypes()) {
      objectNode.put("operationTypes", field.getOperationTypes().toString());
    }
    if (field.hasUserName()) {
      objectNode.put("userName", field.getUserName());
    }

    return objectNode;
  }

  public static ObjectNode buildDataHubOperationJson(@Nonnull final DataHubOperationSpec field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    if (field.hasOperationTypes()) {
      objectNode.put("operationTypes", field.getOperationTypes().toString());
    }
    if (field.hasCustomOperationTypes()) {
      objectNode.put("customOperationTypes", field.getCustomOperationTypes().toString());
    }

    return objectNode;
  }

  public static ObjectNode buildSchemaFieldSpecJson(@Nonnull final SchemaFieldSpec field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("path", field.getPath());
    objectNode.put("type", field.getType());
    objectNode.put("nativeType", field.getNativeType());

    return objectNode;
  }

  public static ObjectNode buildFreshnessFieldSpecJson(@Nonnull final FreshnessFieldSpec field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("path", field.getPath());
    objectNode.put("type", field.getType());
    objectNode.put("nativeType", field.getNativeType());
    if (field.hasKind()) {
      objectNode.put("kind", field.getKind().toString());
    }

    return objectNode;
  }

  public static ObjectNode buildFreshnessCronScheduleJson(
      @Nonnull final FreshnessCronSchedule field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("cron", field.getCron());
    objectNode.put("timezone", field.getTimezone());
    if (field.hasWindowStartOffsetMs()) {
      objectNode.put("windowStartOffsetMs", field.getWindowStartOffsetMs());
    }

    return objectNode;
  }

  public static ObjectNode buildFixedIntervalScheduleJson(
      @Nonnull final FixedIntervalSchedule field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("unit", field.getUnit().toString());
    objectNode.put("multiple", field.getMultiple());

    return objectNode;
  }

  public static ObjectNode buildFieldValuesAssertionJson(
      @Nonnull final FieldValuesAssertion fieldValuesAssertion) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("operator", fieldValuesAssertion.getOperator().toString());
    objectNode.put("excludeNulls", fieldValuesAssertion.isExcludeNulls().toString());
    objectNode.put("field", buildSchemaFieldSpecJson(fieldValuesAssertion.getField()));

    final ObjectNode failThresholdNode = objectMapper.createObjectNode();
    failThresholdNode.put("type", fieldValuesAssertion.getFailThreshold().getType().toString());
    failThresholdNode.put("value", fieldValuesAssertion.getFailThreshold().getValue().toString());
    objectNode.put("failThreshold", failThresholdNode);

    if (fieldValuesAssertion.hasTransform()) {
      final ObjectNode transformNode = objectMapper.createObjectNode();
      transformNode.put("type", fieldValuesAssertion.getTransform().getType().toString());
      objectNode.put("transform", transformNode);
    }

    if (fieldValuesAssertion.hasParameters()) {
      objectNode.put(
          "parameters", buildAssertionStdParametersJson(fieldValuesAssertion.getParameters()));
    }

    return objectNode;
  }

  public static ObjectNode buildFreshnessAssertionScheduleJson(
      @Nonnull final FreshnessAssertionSchedule freshnessAssertionSchedule) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("type", freshnessAssertionSchedule.getType().toString());
    if (freshnessAssertionSchedule.hasCron()) {
      objectNode.put("cron", buildFreshnessCronScheduleJson(freshnessAssertionSchedule.getCron()));
    }
    if (freshnessAssertionSchedule.hasFixedInterval()) {
      objectNode.put(
          "fixedInterval",
          buildFixedIntervalScheduleJson(freshnessAssertionSchedule.getFixedInterval()));
    }

    return objectNode;
  }

  public static ObjectNode buildIncrementingSegmentSpecJson(
      @Nonnull final IncrementingSegmentSpec field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("field", buildSchemaFieldSpecJson(field.getField()));
    if (field.hasTransformer()) {
      final ObjectNode transformerNode = objectMapper.createObjectNode();
      transformerNode.put("type", field.getTransformer().getType().toString());
      if (field.getTransformer().hasNativeType()) {
        transformerNode.put("nativeType", field.getTransformer().getNativeType().toString());
      }
      objectNode.put("transformer", transformerNode);
    }

    return objectNode;
  }

  public static ObjectNode buildFieldMetricAssertionJson(
      @Nonnull final FieldMetricAssertion fieldMetricAssertion) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("operator", fieldMetricAssertion.getOperator().toString());
    objectNode.put("metric", fieldMetricAssertion.getMetric().toString());
    objectNode.put("field", buildSchemaFieldSpecJson(fieldMetricAssertion.getField()));

    if (fieldMetricAssertion.hasParameters()) {
      objectNode.put(
          "parameters", buildAssertionStdParametersJson(fieldMetricAssertion.getParameters()));
    }

    return objectNode;
  }

  public static ObjectNode buildDatasetFreshnessParametersJson(
      @Nonnull final DatasetFreshnessAssertionParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("sourceType", parameters.getSourceType().toString());
    if (parameters.hasField()) {
      objectNode.put("field", buildFreshnessFieldSpecJson(parameters.getField()));
    }
    if (parameters.hasAuditLog()) {
      objectNode.put("auditLog", buildAuditLogSpecJson(parameters.getAuditLog()));
    }
    if (parameters.hasDataHubOperation()) {
      objectNode.put(
          "dataHubOperation", buildDataHubOperationJson(parameters.getDataHubOperation()));
    }

    return objectNode;
  }

  public static ObjectNode buildDatasetFieldParametersJson(
      @Nonnull final DatasetFieldAssertionParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("sourceType", parameters.getSourceType().toString());
    if (parameters.hasChangedRowsField()) {
      objectNode.put(
          "changedRowsField", buildFreshnessFieldSpecJson(parameters.getChangedRowsField()));
    }

    return objectNode;
  }

  public static ObjectNode buildDatasetSchemaParametersJson(
      @Nonnull final DatasetSchemaAssertionParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("sourceType", parameters.getSourceType().toString());
    return objectNode;
  }

  public static ObjectNode buildAssertionEvaluationParametersJson(
      @Nonnull final AssertionEvaluationParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("type", parameters.getType().toString());
    if (parameters.hasDatasetFreshnessParameters()) {
      objectNode.put(
          "datasetFreshnessParameters",
          buildDatasetFreshnessParametersJson(parameters.getDatasetFreshnessParameters()));
    }
    if (parameters.hasDatasetVolumeParameters()) {
      final ObjectNode datasetVolumeParametersNode = objectMapper.createObjectNode();
      datasetVolumeParametersNode.put(
          "sourceType", parameters.getDatasetVolumeParameters().getSourceType().toString());
      objectNode.put("datasetVolumeParameters", datasetVolumeParametersNode);
    }
    if (parameters.hasDatasetFieldParameters()) {
      objectNode.put(
          "datasetFieldParameters",
          buildDatasetFieldParametersJson(parameters.getDatasetFieldParameters()));
    }
    if (parameters.hasDatasetSchemaParameters()) {
      objectNode.put(
          "datasetSchemaParameters",
          buildDatasetSchemaParametersJson(parameters.getDatasetSchemaParameters()));
    }

    return objectNode;
  }

  public static String buildTestFreshnessAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final FreshnessAssertionInfo freshnessAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ObjectNode assertionNode = objectMapper.createObjectNode();
    final ObjectNode freshnessAssertionNode = objectMapper.createObjectNode();
    final ObjectNode paramNode = objectMapper.createObjectNode();

    freshnessAssertionNode.put("type", freshnessAssertionInfo.getType().toString());
    freshnessAssertionNode.put(
        "schedule", buildFreshnessAssertionScheduleJson(freshnessAssertionInfo.getSchedule()));

    if (freshnessAssertionInfo.hasFilter()) {
      final ObjectNode filterNode = objectMapper.createObjectNode();
      filterNode.put("type", freshnessAssertionInfo.getFilter().getType().toString());
      filterNode.put("sql", freshnessAssertionInfo.getFilter().getSql());
      freshnessAssertionNode.put("filter", filterNode);
    }

    assertionNode.put("freshnessAssertion", freshnessAssertionNode);

    paramNode.put("type", "DATASET_FRESHNESS");
    objectNode.put("type", type);
    objectNode.put("connectionUrn", connectionUrn);
    objectNode.put("entityUrn", asserteeUrn);
    objectNode.put("assertion", assertionNode);
    objectNode.put("parameters", buildAssertionEvaluationParametersJson(parameters));

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public static String buildTestVolumeAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final VolumeAssertionInfo volumeAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ObjectNode assertionNode = objectMapper.createObjectNode();
    final ObjectNode volumeAssertionNode = objectMapper.createObjectNode();
    final ObjectNode paramNode = objectMapper.createObjectNode();

    volumeAssertionNode.put("type", volumeAssertionInfo.getType().toString());
    if (volumeAssertionInfo.hasRowCountTotal()) {
      final ObjectNode rowCountTotalNode = objectMapper.createObjectNode();
      rowCountTotalNode.put(
          "operator", volumeAssertionInfo.getRowCountTotal().getOperator().toString());
      rowCountTotalNode.put(
          "parameters",
          buildAssertionStdParametersJson(volumeAssertionInfo.getRowCountTotal().getParameters()));
      volumeAssertionNode.put("rowCountTotal", rowCountTotalNode);
    }
    if (volumeAssertionInfo.hasRowCountChange()) {
      final ObjectNode rowCountChangeNode = objectMapper.createObjectNode();
      rowCountChangeNode.put("type", volumeAssertionInfo.getRowCountChange().getType().toString());
      rowCountChangeNode.put(
          "operator", volumeAssertionInfo.getRowCountChange().getOperator().toString());
      rowCountChangeNode.put(
          "parameters",
          buildAssertionStdParametersJson(volumeAssertionInfo.getRowCountChange().getParameters()));
      volumeAssertionNode.put("rowCountChange", rowCountChangeNode);
    }
    if (volumeAssertionInfo.hasIncrementingSegmentRowCountTotal()) {
      final ObjectNode incRowCountTotalNode = objectMapper.createObjectNode();
      incRowCountTotalNode.put(
          "segment",
          buildIncrementingSegmentSpecJson(
              volumeAssertionInfo.getIncrementingSegmentRowCountTotal().getSegment()));
      incRowCountTotalNode.put(
          "operator",
          volumeAssertionInfo.getIncrementingSegmentRowCountTotal().getOperator().toString());
      incRowCountTotalNode.put(
          "parameters",
          buildAssertionStdParametersJson(
              volumeAssertionInfo.getIncrementingSegmentRowCountTotal().getParameters()));
      volumeAssertionNode.put("incrementingSegmentRowCountTotal", incRowCountTotalNode);
    }
    if (volumeAssertionInfo.hasIncrementingSegmentRowCountChange()) {
      final ObjectNode incRowCountChangeNode = objectMapper.createObjectNode();
      incRowCountChangeNode.put(
          "segment",
          buildIncrementingSegmentSpecJson(
              volumeAssertionInfo.getIncrementingSegmentRowCountChange().getSegment()));
      incRowCountChangeNode.put(
          "type", volumeAssertionInfo.getIncrementingSegmentRowCountChange().getType().toString());
      incRowCountChangeNode.put(
          "operator",
          volumeAssertionInfo.getIncrementingSegmentRowCountChange().getOperator().toString());
      incRowCountChangeNode.put(
          "parameters",
          buildAssertionStdParametersJson(
              volumeAssertionInfo.getIncrementingSegmentRowCountChange().getParameters()));
      volumeAssertionNode.put("incrementingSegmentRowCountChange", incRowCountChangeNode);
    }

    if (volumeAssertionInfo.hasFilter()) {
      final ObjectNode filterNode = objectMapper.createObjectNode();
      filterNode.put("type", volumeAssertionInfo.getFilter().getType().toString());
      filterNode.put("sql", volumeAssertionInfo.getFilter().getSql());
      volumeAssertionNode.put("filter", filterNode);
    }
    assertionNode.put("volumeAssertion", volumeAssertionNode);

    paramNode.put("type", "DATASET_VOLUME");
    objectNode.put("type", type);
    objectNode.put("connectionUrn", connectionUrn);
    objectNode.put("entityUrn", asserteeUrn);
    objectNode.put("assertion", assertionNode);
    objectNode.put("parameters", buildAssertionEvaluationParametersJson(parameters));

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public static String buildTestSqlAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final SqlAssertionInfo sqlAssertionInfo)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ObjectNode assertionNode = objectMapper.createObjectNode();
    final ObjectNode sqlAssertionNode = objectMapper.createObjectNode();
    final ObjectNode paramNode = objectMapper.createObjectNode();

    sqlAssertionNode.put("type", sqlAssertionInfo.getType().toString());
    sqlAssertionNode.put("statement", sqlAssertionInfo.getStatement());
    if (sqlAssertionInfo.hasChangeType()) {
      sqlAssertionNode.put("changeType", sqlAssertionInfo.getChangeType().toString());
    }
    sqlAssertionNode.put("operator", sqlAssertionInfo.getOperator().toString());
    sqlAssertionNode.put(
        "parameters", buildAssertionStdParametersJson(sqlAssertionInfo.getParameters()));
    assertionNode.put("sqlAssertion", sqlAssertionNode);

    paramNode.put("type", "DATASET_SQL");

    objectNode.put("type", type);
    objectNode.put("connectionUrn", connectionUrn);
    objectNode.put("entityUrn", asserteeUrn);
    objectNode.put("assertion", assertionNode);
    objectNode.put("parameters", paramNode);

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public static String buildTestFieldAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final FieldAssertionInfo fieldAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ObjectNode assertionNode = objectMapper.createObjectNode();
    final ObjectNode fieldAssertionNode = objectMapper.createObjectNode();
    final ObjectNode paramNode = objectMapper.createObjectNode();

    fieldAssertionNode.put("type", fieldAssertionInfo.getType().toString());
    if (fieldAssertionInfo.hasFilter()) {
      final ObjectNode filterNode = objectMapper.createObjectNode();
      filterNode.put("type", fieldAssertionInfo.getFilter().getType().toString());
      filterNode.put("sql", fieldAssertionInfo.getFilter().getSql());
      fieldAssertionNode.put("filter", filterNode);
    }

    if (fieldAssertionInfo.getType() == FieldAssertionType.FIELD_VALUES) {
      if (fieldAssertionInfo.hasFieldValuesAssertion()) {
        fieldAssertionNode.put(
            "fieldValuesAssertion",
            buildFieldValuesAssertionJson(fieldAssertionInfo.getFieldValuesAssertion()));
      } else {
        throw new IllegalArgumentException(
            "fieldValuesAssertion is required when type is FIELD_VALUES");
      }
    }

    if (fieldAssertionInfo.getType() == FieldAssertionType.FIELD_METRIC) {
      if (fieldAssertionInfo.hasFieldMetricAssertion()) {
        fieldAssertionNode.put(
            "fieldMetricAssertion",
            buildFieldMetricAssertionJson(fieldAssertionInfo.getFieldMetricAssertion()));
      } else {
        throw new IllegalArgumentException(
            "fieldMetricAssertion is required when type is FIELD_METRIC");
      }
    }

    assertionNode.put("fieldAssertion", fieldAssertionNode);
    paramNode.put("type", "DATASET_FIELD");
    objectNode.put("type", type);
    objectNode.put("connectionUrn", connectionUrn);
    objectNode.put("entityUrn", asserteeUrn);
    objectNode.put("assertion", assertionNode);
    objectNode.put("parameters", buildAssertionEvaluationParametersJson(parameters));

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public static String buildTestSchemaAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final SchemaAssertionInfo schemaAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ObjectNode assertionNode = objectMapper.createObjectNode();
    final ObjectNode schemaAssertionNode = objectMapper.createObjectNode();
    final ObjectNode paramNode = objectMapper.createObjectNode();

    schemaAssertionNode.put("compatibility", schemaAssertionInfo.getCompatibility().toString());
    ArrayNode fields = objectMapper.createArrayNode();
    schemaAssertionInfo
        .getSchema()
        .getFields()
        .forEach(
            field -> {
              fields.add(buildSchemaAssertionFieldJson(field));
            });
    schemaAssertionNode.put("fields", fields);

    assertionNode.put("schemaAssertion", schemaAssertionNode);
    paramNode.put("type", "DATASET_SCHEMA");
    objectNode.put("type", type);
    objectNode.put("connectionUrn", connectionUrn);
    objectNode.put("entityUrn", asserteeUrn);
    objectNode.put("assertion", assertionNode);
    objectNode.put("parameters", buildAssertionEvaluationParametersJson(parameters));

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public static ObjectNode buildSchemaAssertionFieldJson(@Nonnull final SchemaField field) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("path", field.getFieldPath());
    objectNode.put("type", AssertionUtils.mapSchemaFieldDataType(field.getType()));
    objectNode.put("nativeType", field.getNativeDataType());

    return objectNode;
  }

  public static AssertionResult buildTestAssertionResult(@Nonnull final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      ObjectNode json = (ObjectNode) mapper.readTree(jsonStr);
      return extractAssertionResult(json);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse JSON received from the Monitors service! %s");
    }
  }

  public static String buildRunAssertionsBodyJson(
      @Nonnull final List<Urn> assertionUrns,
      final boolean dryRun,
      @Nonnull final Map<String, String> parameters,
      final boolean async)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ArrayNode assertionUrnsNode = objectMapper.createArrayNode();
    for (Urn urn : assertionUrns) {
      assertionUrnsNode.add(urn.toString());
    }
    objectNode.set("urns", assertionUrnsNode);
    objectNode.set("dryRun", BooleanNode.valueOf(dryRun));
    objectNode.set("async", BooleanNode.valueOf(async));
    if (!parameters.isEmpty()) {
      final ObjectNode parametersNode = objectMapper.createObjectNode();
      parameters.forEach(parametersNode::put);
      objectNode.set("parameters", parametersNode);
    }
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public static Map<Urn, AssertionResult> buildRunAssertionsResult(
      @Nonnull List<Urn> assertionUrns, @Nonnull final String jsonStr) {
    final ObjectMapper mapper = new ObjectMapper();
    final Map<Urn, AssertionResult> results = new HashMap<>();
    try {
      ObjectNode json = (ObjectNode) mapper.readTree(jsonStr);
      ArrayNode assertionResults = (ArrayNode) json.get("results");
      for (Urn urn : assertionUrns) {
        for (int i = 0; i < assertionResults.size(); i++) {
          ObjectNode result = (ObjectNode) assertionResults.get(i);
          if (result.get("urn").asText().equals(urn.toString())) {
            results.put(urn, extractAssertionResult((ObjectNode) result.get("result")));
            break;
          }
        }
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to parse run assertions result received from the Monitors service! %s",
              e.getMessage()));
    }
    return results;
  }

  private static AssertionResult extractAssertionResult(ObjectNode json) {
    final ObjectMapper mapper = new ObjectMapper();
    final AssertionResult assertionResult = new AssertionResult();

    assertionResult.setType(AssertionResultType.valueOf(json.get("type").asText()));
    if (json.has("rowCount") && !json.get("rowCount").isNull()) {
      assertionResult.setRowCount(json.get("rowCount").asLong());
    }
    if (json.has("missingCount") && !json.get("missingCount").isNull()) {
      assertionResult.setMissingCount(json.get("missingCount").asLong());
    }
    if (json.has("unexpectedCount") && !json.get("unexpectedCount").isNull()) {
      assertionResult.setUnexpectedCount(json.get("unexpectedCount").asLong());
    }
    if (json.has("actualAggValue") && !json.get("actualAggValue").isNull()) {
      assertionResult.setActualAggValue(Float.valueOf(json.get("actualAggValue").asText()));
    }
    if (json.has("externalUrl") && !json.get("externalUrl").isNull()) {
      assertionResult.setExternalUrl(json.get("externalUrl").asText());
    }
    if (json.has("nativeResults") && !json.get("nativeResults").isNull()) {
      Map<String, String> properties =
          mapper.convertValue(
              json.get("nativeResults"), new TypeReference<Map<String, String>>() {});
      assertionResult.setNativeResults(new StringMap(properties));
    }
    if (json.has("error") && !json.get("error").isNull()) {
      final AssertionResultError assertionResultError = new AssertionResultError();
      assertionResultError.setType(
          AssertionResultErrorType.valueOf(json.get("error").get("type").asText()));
      if (json.get("error").has("properties") && !json.get("error").get("properties").isNull()) {
        Map<String, String> properties =
            mapper.convertValue(
                json.get("error").get("properties"), new TypeReference<Map<String, String>>() {});
        assertionResultError.setProperties(new StringMap(properties));
      }
      assertionResult.setError(assertionResultError);
    }

    return assertionResult;
  }

  private MonitorServiceUtils() {}
}
