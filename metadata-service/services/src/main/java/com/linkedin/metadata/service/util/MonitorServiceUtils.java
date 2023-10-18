package com.linkedin.metadata.service.util;

import java.util.Map;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.linkedin.assertion.FreshnessFieldSpec;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.data.template.StringMap;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.schema.SchemaFieldSpec;

public class MonitorServiceUtils {
  public static ObjectNode buildAssertionStdParametersJson(@Nonnull final AssertionStdParameters parameters) {
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

  public static ObjectNode buildFieldValuesAssertionJson(@Nonnull final FieldValuesAssertion fieldValuesAssertion) {
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
      objectNode.put("parameters", buildAssertionStdParametersJson(fieldValuesAssertion.getParameters()));
    }

    return objectNode;
  }

  public static ObjectNode buildFieldMetricAssertionJson(@Nonnull final FieldMetricAssertion fieldMetricAssertion) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("operator", fieldMetricAssertion.getOperator().toString());
    objectNode.put("metric", fieldMetricAssertion.getMetric().toString());
    objectNode.put("field", buildSchemaFieldSpecJson(fieldMetricAssertion.getField()));

    if (fieldMetricAssertion.hasParameters()) {
      objectNode.put("parameters", buildAssertionStdParametersJson(fieldMetricAssertion.getParameters()));
    }

    return objectNode;
  }

  public static ObjectNode buildDatasetFieldParametersJson(@Nonnull final DatasetFieldAssertionParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("sourceType", parameters.getSourceType().toString());
    if (parameters.hasChangedRowsField()) {
      objectNode.put("changedRowsField", buildFreshnessFieldSpecJson(parameters.getChangedRowsField()));
    }

    return objectNode;
  }

  public static ObjectNode buildAssertionEvaluationParametersJson(@Nonnull final AssertionEvaluationParameters parameters) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();

    objectNode.put("type", parameters.getType().toString());
    if (parameters.hasDatasetFieldParameters()) {
      objectNode.put("datasetFieldParameters", buildDatasetFieldParametersJson(parameters.getDatasetFieldParameters()));
    }

    return objectNode;
  }

  public static String buildTestSqlAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final SqlAssertionInfo sqlAssertionInfo
  ) throws Exception {
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
    sqlAssertionNode.put("parameters", buildAssertionStdParametersJson(sqlAssertionInfo.getParameters()));
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
      @Nonnull final AssertionEvaluationParameters parameters
  ) throws Exception {
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
        fieldAssertionNode.put("fieldValuesAssertion", buildFieldValuesAssertionJson(fieldAssertionInfo.getFieldValuesAssertion()));
      } else {
        throw new IllegalArgumentException("fieldValuesAssertion is required when type is FIELD_VALUES");
      }
    }

    if (fieldAssertionInfo.getType() == FieldAssertionType.FIELD_METRIC) {
      if (fieldAssertionInfo.hasFieldMetricAssertion()) {
        fieldAssertionNode.put("fieldMetricAssertion", buildFieldMetricAssertionJson(fieldAssertionInfo.getFieldMetricAssertion()));
      } else {
        throw new IllegalArgumentException("fieldMetricAssertion is required when type is FIELD_METRIC");
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

  public static AssertionResult buildTestAssertionResult(@Nonnull final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      ObjectNode json = (ObjectNode) mapper.readTree(jsonStr);
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
        Map<String, String> properties = mapper.convertValue(json.get("nativeResults"), new TypeReference<Map<String, String>>() { });
        assertionResult.setNativeResults(new StringMap(properties));
      }
      if (json.has("error") && !json.get("error").isNull()) {
        final AssertionResultError assertionResultError = new AssertionResultError();
        assertionResultError.setType(AssertionResultErrorType.valueOf(json.get("error").get("type").asText()));
        if (json.get("error").has("properties") && !json.get("error").get("properties").isNull()) {
          Map<String, String> properties = mapper.convertValue(json.get("error").get("properties"), new TypeReference<Map<String, String>>() { });
          assertionResultError.setProperties(new StringMap(properties));
        }
        assertionResult.setError(assertionResultError);
      }

      return assertionResult;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON received from the Monitors service! %s");
    }
  }
    
    private MonitorServiceUtils() { }
}
