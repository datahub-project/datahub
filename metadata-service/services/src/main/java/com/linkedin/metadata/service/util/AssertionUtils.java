package com.linkedin.metadata.service.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.SchemaAssertionCompatibility;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.timeseries.CalendarInterval;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionUtils {

  // FIELD_ASSERTIONS - FIELD_VALUES
  private static final String FIELD_ASSERTIONS_INVALID_ROWS_VALUE_KEY_NAME = "Invalid Rows";
  private static final String FIELD_ASSERTIONS_FIELD_VALUES_THRESHOLD_VALUE_KEY_NAME =
      "Threshold Value";

  // FIELD_ASSERTIONS - METRIC_VALUES
  private static final String FIELD_ASSERTIONS_METRIC_VALUE_KEY_NAME = "Metric Value";

  // SQL_ASSERTIONS
  private static final String SQL_ASSERTIONS_METRIC_VALUE_KEY_NAME = "Value";
  private static final String SQL_ASSERTIONS_PREVIOUS_METRIC_VALUE_KEY_NAME = "Previous Value";

  // VOLUME_ASSERTIONS
  private static final String VOLUME_ASSERTIONS_PREVIOUS_ROW_COUNT_KEY_NAME = "Previous Row Count";

  // SCHEMA_ASSERTIONS
  private static final String SCHEMA_ASSERTIONS_EXTRA_FIELDS_IN_ACTUAL_KEY_NAME =
      "Extra Fields in Actual";
  private static final String SCHEMA_ASSERTIONS_EXTRA_FIELDS_IN_EXPECTED_KEY_NAME =
      "Extra Fields in Expected";
  private static final String SCHEMA_ASSERTIONS_MISMATCHED_TYPE_FIELDS_KEY_NAME =
      "Mismatched Type Fields";

  private static final int MAX_SQL_PREVIEW_LENGTH = 50;

  public static String buildAssertionDescription(
      @Nonnull final Urn assertionUrn, @Nonnull final AssertionInfo info) {

    // If assertion has a description, always just use that!
    if (info.hasDescription()) {
      return info.getDescription();
    }

    // Else try to build one.
    switch (info.getType()) {
      case FRESHNESS:
        return buildFreshnessAssertionDescription(info.getFreshnessAssertion());
      case VOLUME:
        return buildVolumeAssertionDescription(info.getVolumeAssertion());
      case FIELD:
        return buildFieldAssertionDescription(info.getFieldAssertion());
      case SQL:
        return buildSqlAssertionDescription(info.getSqlAssertion());
      case DATA_SCHEMA:
        return buildSchemaAssertionDescription(info.getSchemaAssertion());
      case DATASET:
        return buildDatasetAssertionDescription(info.getDatasetAssertion());
      default:
        // Unknown type - return the raw urn.
        log.warn(
            String.format(
                "Attempted to generate description for unsupported assertion of type %s. Returning the raw urn %s",
                info.getType(), assertionUrn));
        return assertionUrn.toString();
    }
  }

  public static String buildAssertionResultReason(
      @Nonnull final Urn assertionUrn,
      @Nonnull AssertionInfo currentInfo,
      @Nonnull final AssertionRunEvent runEvent) {
    // TODO: Handle successes more tactfully here. For now, just return a success message.
    if (runEvent.hasResult()
        && AssertionResultType.SUCCESS.equals(runEvent.getResult().getType())) {
      return "The expectations of the assertion were met.";
    }

    // Extract the Assertion Info conditions as per the last run.
    final AssertionInfo runInfo = runEvent.hasResult() ? runEvent.getResult().getAssertion() : null;

    if (runInfo == null) {
      log.warn(
          String.format(
              "Attempted to generate reason for assertion run event with no assertion info. Returning the raw urn %s",
              assertionUrn));
      return getDefaultAssertionResultReason();
    }

    try {
      // Else build a reason based on the expected conditions + the value.
      switch (runInfo.getType()) {
        case FRESHNESS:
          return buildFreshnessAssertionFailureReason(runInfo.getFreshnessAssertion());
        case VOLUME:
          return buildVolumeAssertionFailureReason(runInfo.getVolumeAssertion(), runEvent);
        case FIELD:
          return buildFieldAssertionFailureReason(runInfo.getFieldAssertion(), runEvent);
        case SQL:
          return buildSqlAssertionFailureReason(runInfo.getSqlAssertion(), runEvent);
        case DATA_SCHEMA:
          return buildSchemaAssertionFailureReason(runInfo.getSchemaAssertion(), runEvent);
        case DATASET:
        default:
          return getDefaultAssertionResultReason();
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to generate reason for assertion run event with assertion info %s. Returning the raw urn %s",
              runInfo, assertionUrn),
          e);
      return getDefaultAssertionResultReason();
    }
  }

  private static String buildFreshnessAssertionDescription(
      @Nonnull final FreshnessAssertionInfo info) {
    FreshnessAssertionSchedule schedule = info.getSchedule();
    FreshnessAssertionScheduleType type = info.getSchedule().getType();
    String freshnessText =
        FreshnessAssertionScheduleType.FIXED_INTERVAL.equals(type)
            ? String.format(
                "in the past %s %s",
                schedule.getFixedInterval().getMultiple(),
                getUnitText(schedule.getFixedInterval().getUnit()))
            : "since the previous check"; // Cron schedule.
    return String.format("Dataset was updated %s", freshnessText);
  }

  private static String getUnitText(@Nonnull final CalendarInterval interval) {
    switch (interval) {
      case SECOND:
        return "seconds";
      case MINUTE:
        return "minutes";
      case HOUR:
        return "hours";
      case DAY:
        return "days";
      case WEEK:
        return "weeks";
      case QUARTER:
        return "quarters";
      case YEAR:
        return "years";
      default:
        log.warn(String.format("Found unknown interval %s", interval));
        return interval.toString();
    }
  }

  private static String buildVolumeAssertionDescription(@Nonnull final VolumeAssertionInfo info) {
    switch (info.getType()) {
      case ROW_COUNT_TOTAL:
        return String.format(
            "Row count %s %s",
            getOperatorText(info.getRowCountTotal().getOperator()),
            getParameterText(info.getRowCountTotal().getParameters()));
      case ROW_COUNT_CHANGE:
        String volumeTypeChange =
            AssertionValueChangeType.ABSOLUTE.equals(info.getRowCountChange().getType())
                ? "change"
                : "percentage change";
        return String.format(
            "Row count %s %s %s",
            volumeTypeChange,
            getOperatorText(info.getRowCountChange().getOperator()),
            getParameterText(info.getRowCountChange().getParameters()));
      case INCREMENTING_SEGMENT_ROW_COUNT_TOTAL:
        // TODO: Account for transformer
        return String.format(
            "Incremental row count %s %s",
            getOperatorText(info.getIncrementingSegmentRowCountTotal().getOperator()),
            getParameterText(info.getIncrementingSegmentRowCountTotal().getParameters()));
      case INCREMENTING_SEGMENT_ROW_COUNT_CHANGE:
        // TODO: Account for transformer
        String incrementalTypeChange =
            AssertionValueChangeType.ABSOLUTE.equals(info.getRowCountChange().getType())
                ? "change"
                : "percentage change";
        return String.format(
            "Incremental row count %s %s %s",
            incrementalTypeChange,
            getOperatorText(info.getIncrementingSegmentRowCountChange().getOperator()),
            getParameterText(info.getIncrementingSegmentRowCountChange().getParameters()));
      default:
        // Unknown volume assertion type - TODO: What to return here?
        log.warn(
            String.format(
                "Attempted to generate description for volume assertion of type %s.",
                info.getType()));
        return "Unknown volume assertion";
    }
  }

  private static String buildFieldAssertionDescription(@Nonnull final FieldAssertionInfo info) {
    switch (info.getType()) {
      case FIELD_METRIC:
        return String.format(
            "*%s* of column '%s' (%s) %s %s",
            getMetricText(info.getFieldMetricAssertion().getMetric()),
            info.getFieldMetricAssertion().getField().getPath(),
            info.getFieldMetricAssertion().getField().getNativeType(),
            getOperatorText(info.getFieldMetricAssertion().getOperator()),
            getParameterText(info.getFieldMetricAssertion().getParameters()));
      case FIELD_VALUES:
        return String.format(
            "Column '%s' (%s) %s %s",
            info.getFieldValuesAssertion().getField().getPath(),
            info.getFieldValuesAssertion().getField().getNativeType(),
            getOperatorText(info.getFieldValuesAssertion().getOperator()),
            getParameterText(info.getFieldValuesAssertion().getParameters()));
      default:
        // Unknown field assertion type - TODO: What to return here?
        log.warn(
            String.format(
                "Attempted to generate description for field assertion of type %s.",
                info.getType()));
        return "Unknown field assertion";
    }
  }

  private static String buildSqlAssertionDescription(@Nonnull final SqlAssertionInfo info) {
    String sql = info.getStatement();
    String truncatedSql =
        sql.length() > MAX_SQL_PREVIEW_LENGTH
            ? String.format("%s...", sql.substring(0, MAX_SQL_PREVIEW_LENGTH))
            : sql;
    switch (info.getType()) {
      case METRIC:
        return String.format(
            "%s %s %s",
            truncatedSql,
            getOperatorText(info.getOperator()),
            getParameterText(info.getParameters()));
      case METRIC_CHANGE:
        String sqlTypeText =
            AssertionValueChangeType.ABSOLUTE.equals(info.getChangeType())
                ? "change"
                : "percentage change";
        return String.format(
            "%s %s %s %s",
            truncatedSql,
            sqlTypeText,
            getOperatorText(info.getOperator()),
            getParameterText(info.getParameters()));
      default:
        // Unknown sql assertion type - TODO: What to return here?
        log.warn(
            String.format(
                "Attempted to generate description for sql assertion of type %s.", info.getType()));
        return "Unknown sql assertion";
    }
  }

  private static String buildDatasetAssertionDescription(DatasetAssertionInfo datasetAssertion) {
    // Short-circuit for external assertions that come pre-existing systems with a specific name
    if (datasetAssertion.hasNativeType()) {
      return datasetAssertion.getNativeType();
    }
    switch (datasetAssertion.getScope()) {
      case DATASET_COLUMN:
        return String.format(
            "Column %s %s",
            datasetAssertion.getFields().get(0).getEntityKey().getParts().get(1),
            getOperatorText(datasetAssertion.getOperator()));
      case DATASET_ROWS:
        return String.format(
            "%s %s %s",
            getAssertionStdAggregationText(datasetAssertion.getAggregation()),
            getOperatorText(datasetAssertion.getOperator()),
            getParameterText(datasetAssertion.getParameters()));
      default:
        // Unknown dataset assertion scope - TODO: What to return here?
        log.warn(
            String.format(
                "Attempted to generate description for native dataset assertion: %s",
                datasetAssertion));
        return "Unknown native assertion";
    }
  }

  private static String buildSchemaAssertionDescription(@Nonnull final SchemaAssertionInfo info) {
    final Integer expectedFieldCount = info.getSchema().getFields().size();
    final SchemaAssertionCompatibility compatibility = info.getCompatibility();
    final String compatibilityText =
        SchemaAssertionCompatibility.EXACT_MATCH.equals(compatibility)
            ? "exactly match"
            : "contain";
    return String.format(
        "Actual table columns %s %s expected columns", compatibilityText, expectedFieldCount);
  }

  private static String getDefaultAssertionResultReason() {
    return "The expectations of the assertion were not met.";
  }

  private static String buildFreshnessAssertionFailureReason(
      @Nonnull final FreshnessAssertionInfo info) {
    FreshnessAssertionSchedule schedule = info.getSchedule();
    FreshnessAssertionScheduleType type = info.getSchedule().getType();
    String freshnessText =
        FreshnessAssertionScheduleType.FIXED_INTERVAL.equals(type)
            ? String.format(
                "in the past %s %s",
                schedule.getFixedInterval().getMultiple(),
                getUnitText(schedule.getFixedInterval().getUnit()))
            : "since the previous check"; // Cron schedule.
    return String.format(
        "Expected table to be updated %s, but no updates were found.", freshnessText);
  }

  private static String buildVolumeAssertionFailureReason(
      @Nonnull final VolumeAssertionInfo info, @Nonnull final AssertionRunEvent runEvent) {
    switch (info.getType()) {
      case ROW_COUNT_TOTAL:
        return String.format(
            "Expected row count %s %s, but found %s.",
            getOperatorText(info.getRowCountTotal().getOperator()),
            getParameterText(info.getRowCountTotal().getParameters()),
            extractRowCountFromResult(runEvent));
      case ROW_COUNT_CHANGE:
        return String.format(
            "Expected row count change %s %s%s, but found previous row count %s and new row count %s.",
            getOperatorText(info.getRowCountChange().getOperator()),
            getParameterText(info.getRowCountChange().getParameters()),
            AssertionValueChangeType.PERCENTAGE.equals(info.getRowCountChange().getType())
                ? "%"
                : "",
            extractPreviousRowCountFromResult(runEvent),
            extractRowCountFromResult(runEvent));
      default:
        log.warn(
            String.format(
                "Attempted to generate reason for volume assertion of type %s. Returning a default message.",
                info.getType()));
        return "The conditions of the volume assertion were not met.";
    }
  }

  private static String extractRowCountFromResult(@Nonnull final AssertionRunEvent runEvent) {
    return runEvent.hasResult() && runEvent.getResult().hasRowCount()
        ? runEvent.getResult().getRowCount().toString()
        : "N/A";
  }

  private static String extractPreviousRowCountFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    return extractNumericMetricFromNativeResults(
        runEvent, VOLUME_ASSERTIONS_PREVIOUS_ROW_COUNT_KEY_NAME);
  }

  private static String extractNumericMetricFromNativeResults(
      @Nonnull final AssertionRunEvent runEvent, @Nonnull final String keyName) {
    if (runEvent.hasResult() && runEvent.getResult().hasNativeResults()) {
      Map<String, String> nativeResults = runEvent.getResult().getNativeResults();
      return nativeResults.get(keyName);
    }
    return "N/A";
  }

  private static String buildFieldAssertionFailureReason(
      @Nonnull final FieldAssertionInfo info, @Nonnull final AssertionRunEvent runEvent) {
    switch (info.getType()) {
      case FIELD_METRIC:
        return String.format(
            "Expected %s of column '%s' %s %s, but found %s.",
            getMetricText(info.getFieldMetricAssertion().getMetric()).toLowerCase(),
            info.getFieldMetricAssertion().getField().getPath(),
            getOperatorText(info.getFieldMetricAssertion().getOperator()),
            getParameterText(info.getFieldMetricAssertion().getParameters()),
            extractFieldMetricFromResult(runEvent));
      case FIELD_VALUES:
        return String.format(
            "Expected column '%s' %s %s, but found %s invalid rows.",
            info.getFieldValuesAssertion().getField().getPath(),
            getOperatorText(info.getFieldValuesAssertion().getOperator()),
            getParameterText(info.getFieldValuesAssertion().getParameters()),
            extractInvalidRowCountFromResult(runEvent));
      default:
        return "The conditions of the field assertion were not met.";
    }
  }

  private static String extractFieldMetricFromResult(@Nonnull final AssertionRunEvent runEvent) {
    return extractNumericMetricFromNativeResults(runEvent, FIELD_ASSERTIONS_METRIC_VALUE_KEY_NAME);
  }

  private static String extractInvalidRowCountFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    return extractNumericMetricFromNativeResults(
        runEvent, FIELD_ASSERTIONS_INVALID_ROWS_VALUE_KEY_NAME);
  }

  private static String buildSqlAssertionFailureReason(
      @Nonnull final SqlAssertionInfo info, @Nonnull final AssertionRunEvent runEvent) {
    switch (info.getType()) {
      case METRIC:
        return String.format(
            "Expected SQL result %s %s, but found %s.",
            getOperatorText(info.getOperator()),
            getParameterText(info.getParameters()),
            extractSqlMetricFromResult(runEvent));
      case METRIC_CHANGE:
        return String.format(
            "Expected SQL result change %s %s%s, but found previous metric value %s and new metric value %s.",
            getOperatorText(info.getOperator()),
            getParameterText(info.getParameters()),
            AssertionValueChangeType.PERCENTAGE.equals(info.getChangeType()) ? "%" : "",
            extractPreviousSqlMetricFromResult(runEvent),
            extractSqlMetricFromResult(runEvent));
      default:
        return "The conditions of the SQL assertion were not met.";
    }
  }

  private static String extractSqlMetricFromResult(@Nonnull final AssertionRunEvent runEvent) {
    return extractNumericMetricFromNativeResults(runEvent, SQL_ASSERTIONS_METRIC_VALUE_KEY_NAME);
  }

  private static String extractPreviousSqlMetricFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    return extractNumericMetricFromNativeResults(
        runEvent, SQL_ASSERTIONS_PREVIOUS_METRIC_VALUE_KEY_NAME);
  }

  private static String buildSchemaAssertionFailureReason(
      @Nonnull final SchemaAssertionInfo info, @Nonnull final AssertionRunEvent runEvent) {
    final String schemaAssertionSuffix = extractSchemaAssertionSuffixFromResult(runEvent);
    return String.format(
        "The expected columns did not match the actual columns. %s", schemaAssertionSuffix);
  }

  private static String extractSchemaAssertionSuffixFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    final List<String> extraFieldsInActual = extractExtraFieldsInActualFromResult(runEvent);
    final List<String> extraFieldsInExpected = extractExtraFieldsInExpectedFromResult(runEvent);
    final List<String> mismatchedTypeFields = extractMismatchedTypeFieldsFromResult(runEvent);

    String finalString = "";

    if (!extraFieldsInActual.isEmpty()) {
      finalString += String.format("Extra fields in actual: %s. ", extraFieldsInActual);
    }
    if (!extraFieldsInExpected.isEmpty()) {
      finalString += String.format("Extra fields in expected: %s. ", extraFieldsInExpected);
    }
    if (!mismatchedTypeFields.isEmpty()) {
      finalString += String.format("Mismatched type fields: %s. ", mismatchedTypeFields);
    }

    return finalString;
  }

  private static List<String> extractExtraFieldsInActualFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    return extractJsonListFromNativeResults(
        runEvent, SCHEMA_ASSERTIONS_EXTRA_FIELDS_IN_ACTUAL_KEY_NAME);
  }

  private static List<String> extractExtraFieldsInExpectedFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    return extractJsonListFromNativeResults(
        runEvent, SCHEMA_ASSERTIONS_EXTRA_FIELDS_IN_EXPECTED_KEY_NAME);
  }

  private static List<String> extractMismatchedTypeFieldsFromResult(
      @Nonnull final AssertionRunEvent runEvent) {
    return extractJsonListFromNativeResults(
        runEvent, SCHEMA_ASSERTIONS_MISMATCHED_TYPE_FIELDS_KEY_NAME);
  }

  @Nonnull
  private static List<String> extractJsonListFromNativeResults(
      @Nonnull final AssertionRunEvent runEvent, @Nonnull final String keyName) {
    if (runEvent.hasResult() && runEvent.getResult().hasNativeResults()) {
      // Extract string, try to deserialize into a json string list.
      Map<String, String> nativeResults = runEvent.getResult().getNativeResults();
      String jsonListString = nativeResults.get(keyName);
      return deserializeJsonList(jsonListString);
    }
    return Collections.emptyList();
  }

  private static List<String> deserializeJsonList(@Nonnull final String jsonListString) {
    // Parse json to list, log warning if it fails. Use the Jackson library.
    try {
      return new ObjectMapper().readValue(jsonListString, new TypeReference<List<String>>() {});
    } catch (Exception e) {
      log.warn(
          String.format(
              "Failed to deserialize json list from string %s. Returning empty list.",
              jsonListString),
          e);
      return Collections.emptyList();
    }
  }

  private static String getOperatorText(@Nonnull final AssertionStdOperator operator) {
    switch (operator) {
      case EQUAL_TO:
        return "is equal to";
      case NOT_EQUAL_TO:
        return "is not equal to";
      case IN:
        return "is in set";
      case NOT_IN:
        return "is not in set";
      case NULL:
        return "is null";
      case NOT_NULL:
        return "is not null";
      case START_WITH:
        return "starts with";
      case END_WITH:
        return "ends with";
      case REGEX_MATCH:
        return "matches regex";
      case IS_TRUE:
        return "is true";
      case IS_FALSE:
        return "is false";
      case CONTAIN:
        return "contains";
      case GREATER_THAN:
        return "greater than";
      case GREATER_THAN_OR_EQUAL_TO:
        return "greater than or equal to";
      case LESS_THAN:
        return "less than";
      case LESS_THAN_OR_EQUAL_TO:
        return "less than or equal to";
      case BETWEEN:
        return "is between";
      case _NATIVE_:
      default:
        return "matches expectations";
    }
  }

  private static String getMetricText(@Nonnull final FieldMetricType metricType) {
    switch (metricType) {
      case MAX:
        return "Max";
      case MIN:
        return "Min";
      case MEAN:
        return "Average";
      case STDDEV:
        return "Standard deviation";
      case MEDIAN:
        return "Median";
      case NEGATIVE_COUNT:
        return "Negative count";
      case NEGATIVE_PERCENTAGE:
        return "Negative percentage";
      case ZERO_COUNT:
        return "Zero count";
      case ZERO_PERCENTAGE:
        return "Zero percentage";
      case NULL_COUNT:
        return "Null count";
      case NULL_PERCENTAGE:
        return "Null percentage";
      case UNIQUE_COUNT:
        return "Unique count";
      case UNIQUE_PERCENTAGE:
        return "Unique percentage";
      case EMPTY_COUNT:
        return "Empty count";
      case EMPTY_PERCENTAGE:
        return "Empty percentage";
      case MAX_LENGTH:
        return "Max length";
      case MIN_LENGTH:
        return "Min length";
      default:
        return "Unknown metric";
    }
  }

  private static String getAssertionStdAggregationText(
      @Nonnull final AssertionStdAggregation aggregation) {
    switch (aggregation) {
      case MAX:
        return "Max";
      case MIN:
        return "Min";
      case SUM:
        return "Sum";
      case MEAN:
        return "Mean";
      case MEDIAN:
        return "Median";
      case STDDEV:
        return "Standard deviation";
      case COLUMNS:
        return "Columns";
      case IDENTITY:
        return "Column";
      case NULL_COUNT:
        return "Null count";
      case COLUMN_COUNT:
        return "Column count";
      case UNIQUE_COUNT:
        return "Unique count";
      case NULL_PROPORTION:
        return "Ratio of nulls";
      case ROW_COUNT:
        return "Row count";
      case UNIQUE_PROPOTION:
      case UNIQUE_PROPORTION:
        return "Ratio of uniques";
      default:
        return "unknown aggregation";
    }
  }

  private static String getParameterText(@Nullable final AssertionStdParameters parameters) {
    if (parameters != null) {
      if (parameters.hasValue()) {
        return parameters.getValue().getValue();
      } else if (parameters.hasMinValue() && parameters.hasMaxValue()) {
        return String.format(
            "%s and %s", parameters.getMinValue().getValue(), parameters.getMaxValue().getValue());
      }
    }
    return "";
  }

  public static String getAssertionTypeName(final String assertionType) {
    if (AssertionType.DATASET.toString().equals(assertionType)) {
      return "External";
    } else if (AssertionType.FRESHNESS.toString().equals(assertionType)) {
      return "Freshness";
    } else if (AssertionType.VOLUME.toString().equals(assertionType)) {
      return "Volume";
    } else if (AssertionType.FIELD.toString().equals(assertionType)) {
      return "Column";
    } else if (AssertionType.SQL.toString().equals(assertionType)) {
      return "Custom SQL";
    } else if (AssertionType.DATA_SCHEMA.toString().equals(assertionType)) {
      return "Schema";
    } else {
      // Unrecognized type. Prefix text.
      return "";
    }
  }

  public static String getAssertionResultEmoji(final String result) {
    if (AssertionResultType.SUCCESS.toString().equals(result)) {
      return ":white_check_mark:";
    } else if (AssertionResultType.FAILURE.toString().equals(result)) {
      return ":x:";
    } else if (AssertionResultType.ERROR.toString().equals(result)) {
      return ":warning:";
    } else {
      // Unrecognized type. No icon.
      return "";
    }
  }

  public static String getAssertionResultString(final String result) {
    if (AssertionResultType.SUCCESS.toString().equals(result)) {
      return "passed";
    } else if (AssertionResultType.FAILURE.toString().equals(result)) {
      return "failed";
    } else if (AssertionResultType.ERROR.toString().equals(result)) {
      return "completed with errors";
    } else {
      // Unrecognized type.
      return "completed";
    }
  }

  public static String mapSchemaFieldDataType(
      @Nonnull final com.linkedin.schema.SchemaFieldDataType dataTypeUnion) {
    final com.linkedin.schema.SchemaFieldDataType.Type type = dataTypeUnion.getType();
    if (type.isBytesType()) {
      return "BYTES";
    } else if (type.isFixedType()) {
      return "FIXED";
    } else if (type.isBooleanType()) {
      return "BOOLEAN";
    } else if (type.isStringType()) {
      return "STRING";
    } else if (type.isNumberType()) {
      return "NUMBER";
    } else if (type.isDateType()) {
      return "DATE";
    } else if (type.isTimeType()) {
      return "TIME";
    } else if (type.isEnumType()) {
      return "ENUM";
    } else if (type.isNullType()) {
      return "NULL";
    } else if (type.isArrayType()) {
      return "ARRAY";
    } else if (type.isMapType()) {
      return "MAP";
    } else if (type.isRecordType()) {
      return "STRUCT";
    } else if (type.isUnionType()) {
      return "UNION";
    } else {
      throw new RuntimeException(
          String.format(
              "Unrecognized SchemaFieldDataType provided %s", type.memberType().toString()));
    }
  }

  private AssertionUtils() {}
}
