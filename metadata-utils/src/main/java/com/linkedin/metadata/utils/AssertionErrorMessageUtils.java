package com.linkedin.metadata.utils;

import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.monitor.MonitorErrorType;
import javax.annotation.Nullable;

public final class AssertionErrorMessageUtils {

  private AssertionErrorMessageUtils() {}

  @Nullable
  public static String recommendedActionForMonitorError(@Nullable MonitorErrorType type) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case TRAINING_DATA_INSUFFICIENT:
        return "Check back in a bit—training will start automatically once we have enough historical data.";
      case PREDICTION_NOT_CONFIDENT:
        return "Keep waiting — confidence will improve as more data comes in.";
      case INPUT_DATA_INSUFFICIENT:
        return "This usually resolves on its own. If it persists, check your data source connection.";
      case INPUT_DATA_INVALID:
        return "Verify input data format and required fields for training.";
      case INVALID_PARAMETERS:
        return "Check the assertion configuration and fill in any missing fields.";
      case MODEL_CREATION_FAILED:
      case MODEL_TRAINING_FAILED:
      case MODEL_EVALUATION_FAILED:
      case PREDICTION_FORMAT_ERROR:
      case PERSISTENCE_FAILED:
        return "This is usually temporary. If it persists, contact support.";
      case UNKNOWN:
      case $UNKNOWN:
        return "If this persists, contact support.";
    }
    throw new IllegalStateException(String.format("Unhandled monitor error type %s", type));
  }

  @Nullable
  public static String recommendedActionForEvaluationError(
      @Nullable AssertionResultErrorType type) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case SOURCE_CONNECTION_ERROR:
        return "Check that your data source credentials are valid and the service is accessible.";
      case SOURCE_QUERY_FAILED:
        return "The data source might be temporarily unavailable, or the query needs adjustment.";
      case INSUFFICIENT_DATA:
        return "Make sure your data source has recent data and is being ingested properly.";
      case INVALID_PARAMETERS:
      case MISSING_EVALUATION_PARAMETERS:
        return "Check the assertion configuration and fill in any missing fields.";
      case INVALID_SOURCE_TYPE:
        return "Choose a different assertion type or check which types are supported for your data source.";
      case METRIC_RESOLVER_INVALID_SOURCE_TYPE:
        return "This source type might not be fully supported yet. Contact support for details";
      case UNSUPPORTED_PLATFORM:
        return "You'll need to either choose a different assertion type or use a different platform.";
      case CUSTOM_SQL_ERROR:
        return "Make sure your SQL returns exactly 1 row with 1 value.";
      case FIELD_ASSERTION_ERROR:
        return "Check your field-level metrics configuration.";
      case EVALUATOR_NOT_FOUND:
        return "Choose a supported assertion type (Volume, Freshness, Schema, etc.).";
      case METRIC_RESOLVER_UNSUPPORTED_METRIC:
        return "This metric type might not be fully supported yet. Contact support for details.";
      case STATE_PERSISTENCE_FAILED:
      case METRIC_PERSISTENCE_FAILED:
      case RESULT_EMISSION_FAILED:
        return "This is usually temporary. If it persists, contact support.";
      case UNKNOWN_ERROR:
      case $UNKNOWN:
        return "If this persists, contact support.";
    }
    throw new IllegalStateException(
        String.format("Unhandled assertion result error type %s", type));
  }

  @Nullable
  public static String displayMessageForMonitorError(@Nullable MonitorErrorType type) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case TRAINING_DATA_INSUFFICIENT:
        return "Still gathering data to train the model.";
      case PREDICTION_NOT_CONFIDENT:
        return "The model needs more data before it can make reliable predictions.";
      case INPUT_DATA_INSUFFICIENT:
        return "Couldn't retrieve enough data to train.";
      case INPUT_DATA_INVALID:
        return "The data format doesn't match what we expected.";
      case INVALID_PARAMETERS:
        return "This assertion is missing required settings.";
      case MODEL_CREATION_FAILED:
        return "Something went wrong while setting up the model.";
      case MODEL_TRAINING_FAILED:
        return "Training failed unexpectedly.";
      case MODEL_EVALUATION_FAILED:
        return "Couldn't evaluate the trained model.";
      case PREDICTION_FORMAT_ERROR:
        return "The model output was in an unexpected format.";
      case PERSISTENCE_FAILED:
        return "Training finished, but we couldn't save the results.";
      case UNKNOWN:
      case $UNKNOWN:
        return "Something unexpected happened during training.";
    }
    throw new IllegalStateException(String.format("Unhandled monitor error type %s", type));
  }

  @Nullable
  public static String displayMessageForEvaluationError(@Nullable AssertionResultErrorType type) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case SOURCE_CONNECTION_ERROR:
        return "Can't connect to your data source.";
      case SOURCE_QUERY_FAILED:
        return "The query to your data source failed.";
      case INSUFFICIENT_DATA:
        return "Not enough data available to evaluate.";
      case INVALID_PARAMETERS:
        return "This assertion is missing required settings.";
      case MISSING_EVALUATION_PARAMETERS:
        return "This assertion is missing evaluation criteria.";
      case INVALID_SOURCE_TYPE:
        return "This data source type isn't supported for your assertion.";
      case METRIC_RESOLVER_INVALID_SOURCE_TYPE:
        return "Can't resolve metrics for this data source type.";
      case UNSUPPORTED_PLATFORM:
        return "This platform doesn't support this assertion type.";
      case CUSTOM_SQL_ERROR:
        return "Your custom SQL query didn't return the expected format.";
      case FIELD_ASSERTION_ERROR:
        return "Field metrics query returned unexpected data.";
      case EVALUATOR_NOT_FOUND:
        return "This assertion type isn't recognized.";
      case METRIC_RESOLVER_UNSUPPORTED_METRIC:
        return "Can't resolve metrics for this metric type.";
      case STATE_PERSISTENCE_FAILED:
        return "The evaluation ran but couldn't save results.";
      case METRIC_PERSISTENCE_FAILED:
        return "Collected metrics but couldn't save them.";
      case RESULT_EMISSION_FAILED:
        return "Couldn't record the evaluation results.";
      case UNKNOWN_ERROR:
      case $UNKNOWN:
        return "Something unexpected happened during evaluation.";
    }
    throw new IllegalStateException(
        String.format("Unhandled assertion result error type %s", type));
  }
}
