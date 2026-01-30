# ruff: noqa: INP001
"""
Inference data loading utilities for the Streamlit Explorer.

This module provides functions to fetch and parse model inference data
(configs, training evaluations, predictions) from DataHub's
AssertionEvaluationContext aspect.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from datahub.metadata.schema_classes import AssertionInferenceDetailsClass

from datahub_executor.common.monitor.inference_v2.inference_utils import (
    ModelConfig,
    parse_inference_details,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
    AnomalyConfigSerializer,
    AnomalyEvalsSerializer,
    ForecastConfigSerializer,
    ForecastEvalsSerializer,
    PreprocessingConfigSerializer,
)

logger = logging.getLogger(__name__)


# =============================================================================
# GraphQL Queries
# =============================================================================

QUERY_EVALUATION_CONTEXT = """
query GetAssertionEvaluationContext($urn: String!) {
    assertion(urn: $urn) {
        urn
        evaluationContext {
            inferenceDetails {
                modelId
                modelVersion
                generatedAt
                parameters {
                    key
                    value
                }
            }
            embeddedAssertions {
                assertion {
                    type
                    volumeAssertion {
                        type
                        entity
                        rowCountTotal {
                            operator
                            parameters {
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                        }
                    }
                    freshnessAssertion {
                        type
                        entity
                        schedule {
                            type
                            fixedInterval {
                                unit
                                multiple
                            }
                        }
                    }
                }
                evaluationTimeWindow {
                    startTimeMillis
                    length {
                        unit
                        multiple
                    }
                }
                context
            }
        }
    }
}
"""

QUERY_MONITOR_EVALUATION_CONTEXT = """
query GetMonitorEvaluationContext($urn: String!) {
    monitor(urn: $urn) {
        urn
        info {
            assertionMonitor {
                assertions {
                    assertion {
                        urn
                        evaluationContext {
                            inferenceDetails {
                                modelId
                                modelVersion
                                generatedAt
                                parameters {
                                    key
                                    value
                                }
                            }
                            embeddedAssertions {
                                evaluationTimeWindow {
                                    startTimeMillis
                                }
                                context
                            }
                        }
                    }
                }
            }
        }
    }
}
"""


# =============================================================================
# Dataclasses for Parsed Results
# =============================================================================


def _parse_model_name_version(
    name: Optional[str], version: Optional[str]
) -> Optional[Dict[str, str]]:
    """Parse model name and version, extracting version from name if it contains @."""
    if not name:
        return None
    if "@" in name:
        base, parsed_version = name.split("@", 1)
        name = base.strip()
        version = parsed_version.strip() or version
    return {"name": name, "version": version or ""}


class InferenceData:
    """Container for parsed inference data from DataHub.

    Attributes:
        model_config: The ModelConfig parsed from inference details
        preprocessing_config: Deserialized preprocessing config (observe-models object or dict)
        forecast_config: Deserialized forecast model config (observe-models object or dict)
        anomaly_config: Deserialized anomaly model config (observe-models object or dict)
        forecast_evals: Deserialized forecast training evaluations
        anomaly_evals: Deserialized anomaly training evaluations
        predictions_df: DataFrame of embedded assertions (predictions)
        entity_urn: The assertion or monitor URN this data belongs to
        generated_at: Timestamp when the model was trained
    """

    def __init__(
        self,
        model_config: Optional[ModelConfig] = None,
        preprocessing_config: Optional[Any] = None,
        forecast_config: Optional[Any] = None,
        anomaly_config: Optional[Any] = None,
        forecast_evals: Optional[Any] = None,
        anomaly_evals: Optional[Any] = None,
        predictions_df: Optional[pd.DataFrame] = None,
        entity_urn: Optional[str] = None,
        generated_at: Optional[int] = None,
    ):
        self.model_config = model_config
        self.preprocessing_config = preprocessing_config
        self.forecast_config = forecast_config
        self.anomaly_config = anomaly_config
        self.forecast_evals = forecast_evals
        self.anomaly_evals = anomaly_evals
        self.predictions_df = predictions_df
        self.entity_urn = entity_urn
        self.generated_at = generated_at

    @property
    def has_inference_data(self) -> bool:
        """Check if any inference data is available."""
        return self.model_config is not None

    @property
    def has_predictions(self) -> bool:
        """Check if prediction data is available."""
        return self.predictions_df is not None and len(self.predictions_df) > 0

    @property
    def forecast_model_info(self) -> Optional[Dict[str, str]]:
        """Get forecast model name and version."""
        if self.model_config is None:
            return None
        return _parse_model_name_version(
            self.model_config.forecast_model_name,
            self.model_config.forecast_model_version,
        )

    @property
    def anomaly_model_info(self) -> Optional[Dict[str, str]]:
        """Get anomaly model name and version."""
        if self.model_config is None:
            return None
        return _parse_model_name_version(
            self.model_config.anomaly_model_name,
            self.model_config.anomaly_model_version,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for storage."""
        return {
            "entity_urn": self.entity_urn,
            "generated_at": self.generated_at,
            "model_config": self.model_config.model_dump()
            if self.model_config
            else None,
            "forecast_model": self.forecast_model_info,
            "anomaly_model": self.anomaly_model_info,
            "has_predictions": self.has_predictions,
            "prediction_count": len(self.predictions_df)
            if self.predictions_df is not None
            else 0,
        }


# =============================================================================
# GraphQL Fetching Functions
# =============================================================================


class SchemaNotSupportedError(Exception):
    """Raised when the server schema doesn't support required fields."""

    pass


# Module-level flag to track if schema is unsupported (avoids repeated requests)
_schema_unsupported_endpoints: set = set()


def _is_schema_validation_error(errors: List[Dict[str, Any]]) -> bool:
    """Check if GraphQL errors are schema validation errors.

    Schema validation errors indicate the server has an older schema
    that doesn't include the requested fields (e.g., evaluationContext).
    """
    for error in errors:
        message = error.get("message", "")
        # Check for common schema validation error patterns
        if "FieldUndefined" in message or "Field" in message and "undefined" in message:
            return True
    return False


def _execute_graphql(
    graphql_url: str,
    headers: Dict[str, str],
    query: str,
    variables: Dict[str, Any],
    timeout: int = 10,
) -> Optional[Dict[str, Any]]:
    """Execute a GraphQL query and return the data.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        query: The GraphQL query string
        variables: Query variables
        timeout: Request timeout in seconds (default 10s for inference queries)

    Returns:
        The 'data' portion of the response, or None on error.

    Raises:
        SchemaNotSupportedError: If the server schema doesn't support the query.
    """
    # Skip if we already know this endpoint doesn't support the schema
    if graphql_url in _schema_unsupported_endpoints:
        return None

    try:
        response = requests.post(
            graphql_url,
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data and data["errors"]:
            error_msgs = [e.get("message", "") for e in data["errors"]]

            # For schema validation errors (e.g., evaluationContext not defined),
            # mark this endpoint as unsupported to avoid future requests
            if _is_schema_validation_error(data["errors"]):
                logger.info(
                    "Server schema doesn't support evaluationContext field. "
                    "Inference data will not be available."
                )
                _schema_unsupported_endpoints.add(graphql_url)
                raise SchemaNotSupportedError(
                    "Server schema doesn't support evaluationContext"
                )

            # Log other errors as warnings
            logger.warning("GraphQL errors: %s", error_msgs)
            return None

        return data.get("data")

    except requests.exceptions.RequestException as e:
        logger.warning("GraphQL request failed: %s", e)
        return None
    except SchemaNotSupportedError:
        raise  # Re-raise schema errors
    except Exception as e:
        logger.warning("Unexpected error in GraphQL query: %s", e)
        return None


def fetch_assertion_evaluation_context(
    graphql_url: str,
    headers: Dict[str, str],
    assertion_urn: str,
) -> Optional[Dict[str, Any]]:
    """Fetch the AssertionEvaluationContext for an assertion.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        assertion_urn: The assertion URN

    Returns:
        The evaluationContext data dict, or None if not found
    """
    variables = {"urn": assertion_urn}
    data = _execute_graphql(graphql_url, headers, QUERY_EVALUATION_CONTEXT, variables)

    if data is None:
        return None

    assertion_data = data.get("assertion")
    if assertion_data is None:
        return None

    return assertion_data.get("evaluationContext")


def fetch_monitor_evaluation_context(
    graphql_url: str,
    headers: Dict[str, str],
    monitor_urn: str,
) -> Optional[Dict[str, Any]]:
    """Fetch the AssertionEvaluationContext via a monitor.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        monitor_urn: The monitor URN

    Returns:
        The evaluationContext data dict, or None if not found
    """
    variables = {"urn": monitor_urn}
    data = _execute_graphql(
        graphql_url, headers, QUERY_MONITOR_EVALUATION_CONTEXT, variables
    )

    if data is None:
        return None

    monitor_data = data.get("monitor")
    if monitor_data is None:
        return None

    # Navigate to assertion's evaluation context
    info = monitor_data.get("info") or {}
    assertion_monitor = info.get("assertionMonitor") or {}
    assertions = assertion_monitor.get("assertions") or []

    if not assertions:
        return None

    # Return the first assertion's evaluation context
    first_assertion = assertions[0].get("assertion") or {}
    return first_assertion.get("evaluationContext")


# =============================================================================
# Parsing Functions
# =============================================================================


def _parse_parameters_map(parameters: List[Dict[str, str]]) -> Dict[str, str]:
    """Convert GraphQL parameters list to dict.

    Args:
        parameters: List of {key, value} dicts from GraphQL

    Returns:
        Dict mapping key to value
    """
    result = {}
    for param in parameters:
        key = param.get("key")
        value = param.get("value")
        if key and value is not None:
            result[key] = value
    return result


def _graphql_to_inference_details(
    graphql_data: Dict[str, Any],
) -> Optional[AssertionInferenceDetailsClass]:
    """Convert GraphQL inference details to AssertionInferenceDetailsClass.

    Args:
        graphql_data: The inferenceDetails dict from GraphQL

    Returns:
        AssertionInferenceDetailsClass, or None if invalid
    """
    if not graphql_data:
        return None

    parameters = graphql_data.get("parameters") or []
    params_dict = _parse_parameters_map(parameters)

    return AssertionInferenceDetailsClass(
        modelId=graphql_data.get("modelId"),
        modelVersion=graphql_data.get("modelVersion"),
        # Training confidence is legacy; inference_v2 uses forecast/anomaly scores.
        confidence=None,
        generatedAt=graphql_data.get("generatedAt"),
        parameters=params_dict,
    )


def _parse_embedded_assertion(
    assertion_data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Parse a single embedded assertion from GraphQL.

    Args:
        assertion_data: The embeddedAssertion dict from GraphQL

    Returns:
        Dict with timestamp_ms and context, or None if invalid
    """
    if not assertion_data:
        return None

    # Parse context map
    context = assertion_data.get("context")
    if isinstance(context, list):
        # Convert list of {key, value} to dict
        context = _parse_parameters_map(context)
    elif not isinstance(context, dict):
        context = None

    # Parse time window
    time_window = assertion_data.get("evaluationTimeWindow") or {}
    start_time_millis = time_window.get("startTimeMillis")

    # For now, return a simplified structure that can be converted to DataFrame
    # The full EmbeddedAssertionClass reconstruction would require more parsing
    return {
        "timestamp_ms": start_time_millis,
        "context": context or {},
    }


def parse_evaluation_context(
    graphql_data: Dict[str, Any],
    entity_urn: Optional[str] = None,
) -> InferenceData:
    """Parse evaluation context from GraphQL response to InferenceData.

    Args:
        graphql_data: The evaluationContext dict from GraphQL
        entity_urn: The entity URN this data belongs to

    Returns:
        InferenceData object with parsed data
    """
    result = InferenceData(entity_urn=entity_urn)

    if not graphql_data:
        return result

    # Parse inference details
    inference_details_data = graphql_data.get("inferenceDetails")
    if inference_details_data:
        inference_details = _graphql_to_inference_details(inference_details_data)
        if inference_details:
            model_config = parse_inference_details(inference_details)
            if model_config:
                result.model_config = model_config
                result.generated_at = model_config.generated_at

                # Deserialize configs
                if model_config.preprocessing_config_json:
                    result.preprocessing_config = (
                        PreprocessingConfigSerializer.deserialize(
                            model_config.preprocessing_config_json
                        )
                    )

                if model_config.forecast_config_json:
                    result.forecast_config = ForecastConfigSerializer.deserialize(
                        model_config.forecast_config_json
                    )

                if model_config.anomaly_config_json:
                    result.anomaly_config = AnomalyConfigSerializer.deserialize(
                        model_config.anomaly_config_json
                    )

                if model_config.forecast_evals_json:
                    result.forecast_evals = ForecastEvalsSerializer.deserialize(
                        model_config.forecast_evals_json
                    )

                if model_config.anomaly_evals_json:
                    result.anomaly_evals = AnomalyEvalsSerializer.deserialize(
                        model_config.anomaly_evals_json
                    )

    # Parse embedded assertions to DataFrame
    embedded_assertions = graphql_data.get("embeddedAssertions") or []
    if embedded_assertions:
        result.predictions_df = _parse_embedded_assertions_to_df(embedded_assertions)

    return result


def _parse_embedded_assertions_to_df(
    assertions: List[Dict[str, Any]],
) -> pd.DataFrame:
    """Convert embedded assertions to DataFrame.

    Determines assertion type from context keys and uses appropriate converter.

    Args:
        assertions: List of embedded assertion dicts from GraphQL

    Returns:
        DataFrame with predictions
    """
    if not assertions:
        return pd.DataFrame()

    records = []
    for assertion in assertions:
        parsed = _parse_embedded_assertion(assertion)
        if parsed and parsed.get("timestamp_ms") is not None:
            record = {"timestamp_ms": parsed["timestamp_ms"]}
            context = parsed.get("context", {})

            # Extract all context values
            for key, value in context.items():
                # Convert to appropriate types
                if key in (
                    "y",
                    "yhat",
                    "yhatLower",
                    "yhatUpper",
                    "anomalyScore",
                    "detectionBandLower",
                    "detectionBandUpper",
                ):
                    try:
                        record[key] = float(value)
                    except (ValueError, TypeError):
                        record[key] = None
                elif key == "isAnomaly":
                    record[key] = str(value).lower() == "true"
                elif key == "isFresh":
                    record[key] = str(value).lower() == "true"
                elif key == "expectedNextEventMillis":
                    try:
                        record[key] = int(value)
                    except (ValueError, TypeError):
                        record[key] = None
                else:
                    record[key] = value

            records.append(record)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Rename columns to match expected format
    column_renames = {
        "yhatLower": "yhat_lower",
        "yhatUpper": "yhat_upper",
        "anomalyScore": "anomaly_score",
        "isAnomaly": "is_anomaly",
        "detectionBandLower": "detection_band_lower",
        "detectionBandUpper": "detection_band_upper",
        "isFresh": "is_fresh",
        "expectedNextEventMillis": "expected_next_event_millis",
    }
    df = df.rename(columns={k: v for k, v in column_renames.items() if k in df.columns})

    return df


# =============================================================================
# High-Level API Functions
# =============================================================================


def fetch_inference_data(
    graphql_url: str,
    headers: Dict[str, str],
    assertion_urn: Optional[str] = None,
    monitor_urn: Optional[str] = None,
) -> InferenceData:
    """Fetch and parse inference data for an assertion or monitor.

    Either assertion_urn or monitor_urn must be provided.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        assertion_urn: The assertion URN (optional)
        monitor_urn: The monitor URN (optional)

    Returns:
        InferenceData object with parsed data
    """
    if assertion_urn:
        context_data = fetch_assertion_evaluation_context(
            graphql_url, headers, assertion_urn
        )
        return parse_evaluation_context(context_data or {}, entity_urn=assertion_urn)
    elif monitor_urn:
        context_data = fetch_monitor_evaluation_context(
            graphql_url, headers, monitor_urn
        )
        return parse_evaluation_context(context_data or {}, entity_urn=monitor_urn)
    else:
        logger.warning("Neither assertion_urn nor monitor_urn provided")
        return InferenceData()


def get_training_metrics_summary(inference_data: InferenceData) -> Dict[str, Any]:
    """Extract training metrics summary from InferenceData.

    Args:
        inference_data: The InferenceData object

    Returns:
        Dict with metrics summary
    """
    summary: Dict[str, Any] = {
        "has_forecast": False,
        "has_anomaly": False,
        "forecast_metrics": {},
        "anomaly_metrics": {},
    }

    # Extract forecast metrics
    if inference_data.forecast_evals is not None:
        summary["has_forecast"] = True

        # Handle both object and dict forms
        evals = inference_data.forecast_evals
        if hasattr(evals, "aggregated") and evals.aggregated:
            agg = evals.aggregated
            summary["forecast_metrics"] = {
                "mae": getattr(agg, "mae", None),
                "rmse": getattr(agg, "rmse", None),
                "mape": getattr(agg, "mape", None),
                "coverage": getattr(agg, "coverage", None),
            }
        elif isinstance(evals, dict) and "aggregated" in evals:
            agg = evals["aggregated"] or {}
            summary["forecast_metrics"] = {
                "mae": agg.get("mae"),
                "rmse": agg.get("rmse"),
                "mape": agg.get("mape"),
                "coverage": agg.get("coverage"),
            }

    # Extract anomaly metrics
    if inference_data.anomaly_evals is not None:
        summary["has_anomaly"] = True

        evals = inference_data.anomaly_evals
        if hasattr(evals, "aggregated") and evals.aggregated:
            agg = evals.aggregated
            summary["anomaly_metrics"] = {
                "precision": getattr(agg, "precision", None),
                "recall": getattr(agg, "recall", None),
                "f1_score": getattr(agg, "f1_score", None),
                "accuracy": getattr(agg, "accuracy", None),
            }
        elif isinstance(evals, dict) and "aggregated" in evals:
            agg = evals["aggregated"] or {}
            summary["anomaly_metrics"] = {
                "precision": agg.get("precision"),
                "recall": agg.get("recall"),
                "f1_score": agg.get("f1_score"),
                "accuracy": agg.get("accuracy"),
            }

    return summary


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Exceptions
    "SchemaNotSupportedError",
    # Data container
    "InferenceData",
    # Fetching functions
    "fetch_assertion_evaluation_context",
    "fetch_monitor_evaluation_context",
    "fetch_inference_data",
    # Parsing functions
    "parse_evaluation_context",
    # Utility functions
    "get_training_metrics_summary",
]
