package com.linkedin.metadata.service.util;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResultType;
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
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.timeseries.CalendarInterval;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionUtils {

  private static final int MAX_SQL_PREVIEW_LENGTH = 50;
  private static String EVALUATES_RELATIONSHIP_NAME = "Evaluates";

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

  private AssertionUtils() {}
}
