package com.linkedin.metadata.service.util;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.timeseries.CalendarInterval;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AssertionUtils {

  private static final int MAX_SQL_PREVIEW_LENGTH = 50;

  public static String buildAssertionDescription(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionInfo info) {

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
        return assertionUrn.toString(); // TODO build richer preview.
      default:
        // Unknown type - return the raw urn.
        log.warn(String.format(
            "Attempted to generate description for unsupported assertion of type %s. Returning the raw urn %s", info.getType(), assertionUrn));
        return assertionUrn.toString();
    }
  }

  private static String buildFreshnessAssertionDescription(@Nonnull final FreshnessAssertionInfo info) {
    FreshnessAssertionSchedule schedule = info.getSchedule();
    FreshnessAssertionScheduleType type = info.getSchedule().getType();
    String freshnessText = FreshnessAssertionScheduleType.FIXED_INTERVAL.equals(type)
        ? String.format("in the past %s %s", schedule.getFixedInterval().getMultiple(), getUnitText(schedule.getFixedInterval().getUnit()))
        : "since the previous check"; // Cron schedule.
    return String.format("Dataset was updated %s", freshnessText);
  }

  private static String getUnitText(@Nonnull final CalendarInterval interval) {
    switch(interval) {
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
    VolumeAssertionType type = info.getType();
    String volumeTypeChange = VolumeAssertionType.ROW_COUNT_CHANGE.equals(type)
        ? AssertionValueChangeType.ABSOLUTE.equals(info.getRowCountChange().getType()) ? "change " : "percentage change "
        : "";
    String operatorText = info.hasRowCountChange()
        ? getOperatorText(info.getRowCountChange().getOperator())
        : getOperatorText(info.getRowCountTotal().getOperator());
    String parameterText = info.hasRowCountChange()
        ? getParameterText(info.getRowCountChange().getParameters())
        : getParameterText(info.getRowCountTotal().getParameters());
    return "Row count " + volumeTypeChange + operatorText + " " + parameterText;
  }

  private static String buildFieldAssertionDescription(@Nonnull final FieldAssertionInfo info) {
    FieldAssertionType type = info.getType();
    String columnName = info.hasFieldValuesAssertion()
        ? info.getFieldValuesAssertion().getField().getPath()
        : info.getFieldMetricAssertion().getField().getPath();
    String columnType = info.hasFieldValuesAssertion()
        ? info.getFieldValuesAssertion().getField().getNativeType()
        : info.getFieldMetricAssertion().getField().getNativeType();
    String operatorText = info.hasFieldValuesAssertion()
        ? getOperatorText(info.getFieldValuesAssertion().getOperator())
        : getOperatorText(info.getFieldMetricAssertion().getOperator());
    String parameterText = info.hasFieldValuesAssertion()
        ? getParameterText(info.getFieldValuesAssertion().getParameters())
        : getParameterText(info.getFieldMetricAssertion().getParameters());
    String columnMetric = FieldAssertionType.FIELD_METRIC.equals(type)
        ? getMetricText(info.getFieldMetricAssertion().getMetric())
        : null;

    return FieldAssertionType.FIELD_VALUES.equals(type)
        ? String.format("Column '%s' (%s) %s %s", columnName, columnType, operatorText, parameterText)
        : String.format("*%s* of column '%s' (%s) %s %s", columnMetric, columnName, columnType, operatorText, parameterText);
  }

  private static String buildSqlAssertionDescription(@Nonnull final SqlAssertionInfo info) {
    String sql = info.getStatement();
    String truncatedSql = sql.length() > MAX_SQL_PREVIEW_LENGTH
        ? String.format("%s...", sql.substring(0, MAX_SQL_PREVIEW_LENGTH))
        : sql;
    SqlAssertionType type = info.getType();
    AssertionValueChangeType changeType = info.getChangeType();
    String sqlTypeText = SqlAssertionType.METRIC_CHANGE.equals(type)
        ? AssertionValueChangeType.ABSOLUTE.equals(changeType) ? "change " : "percentage change "
        : "";
    String operatorText = getOperatorText(info.getOperator());
    String parameterText = getParameterText(info.getParameters());
    return truncatedSql + " " + sqlTypeText + operatorText + " " + parameterText;
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

  private static String getParameterText(@Nullable final AssertionStdParameters parameters) {
    if (parameters != null) {
      if (parameters.hasValue()) {
        return parameters.getValue().getValue();
      } else if (parameters.hasMinValue() && parameters.hasMaxValue()) {
        return String.format("%s and %s", parameters.getMinValue().getValue(), parameters.getMaxValue().getValue());
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

  private AssertionUtils() { }
}
