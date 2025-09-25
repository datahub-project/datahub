from collections import Counter
from typing import Counter as CounterType, List

import mlflow.metrics as mlflow_metrics
import numpy as np
import pandas as pd
from loguru import logger

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.experimentation.chatbot.chatbot import (
    Prompt,
)
from datahub_integrations.experimentation.chatbot.eval_helpers import (
    check_for_invalid_links,
    extract_response_from_history_json,
    extract_valid_urns_from_history,
)
from datahub_integrations.experimentation.chatbot.judge import (
    validate_expected_tool_calls,
)

# Target: Prompt object
# Prediction: ChatHistory JSON string


def expected_tool_calls_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> mlflow_metrics.MetricValue:
    """Validate expected tool calls are present in chat history."""
    all_scores: List[float] = []
    all_justifications: List[str] = []
    judged_scores: List[float] = []

    for prediction, target in zip(predictions, targets, strict=False):
        prompt = Prompt.model_validate_json(target)
        # Skip if no expected_tool_calls defined in target
        if prompt.expected_tool_calls is None:
            all_scores.append(0)
            all_justifications.append("No expected tool calls defined for this prompt")
            continue

        if prediction is None or prediction == "":
            all_scores.append(0)
            all_justifications.append("No chat history to evaluate")
            continue

        try:
            # prediction contains the chat history JSON string
            history = ChatHistory.model_validate_json(prediction)

            # Validate expected tool calls
            validation_result = validate_expected_tool_calls(
                history, prompt.expected_tool_calls
            )

            all_scores.append(1 if validation_result.is_valid else 0)
            all_justifications.append(validation_result.justification)
            judged_scores.append(1 if validation_result.is_valid else 0)

        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.warning(f"Error validating expected tool calls: {e}")
            all_scores.append(0)
            all_justifications.append(f"Error validating tool calls: {str(e)}")

    return mlflow_metrics.MetricValue(
        scores=all_scores,  # type: ignore
        justifications=all_justifications,
        aggregate_results={
            "pass_percentage": (
                100 * sum(judged_scores) / len(judged_scores)
                if len(judged_scores) > 0
                else 0
            ),
            "count": len(judged_scores),
        },
    )


def has_valid_links_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> mlflow_metrics.MetricValue:
    """Check if all DataHub links in response reference valid URNs from tool results."""
    all_scores: List[float] = []
    all_justifications: List[str] = []
    judged_scores: List[float] = []

    for prediction, _target in zip(predictions, targets, strict=False):
        # Extract response from chat history
        response = extract_response_from_history_json(prediction)

        if not response:
            # Skip entries with no response
            all_scores.append(0)
            all_justifications.append("No response to evaluate")
            continue

        history = ChatHistory.model_validate_json(prediction)

        # Extract valid URNs from chat history
        valid_urns = extract_valid_urns_from_history(history)

        # Extract DataHub links from response
        prompt = Prompt.model_validate_json(_target)
        frontend_base_url = f"https://{prompt.instance}.acryl.io"
        invalid_links = check_for_invalid_links(response, valid_urns, frontend_base_url)
        is_valid = len(invalid_links) == 0
        all_scores.append(1 if is_valid else 0)

        if not is_valid:
            all_justifications.append(f"Invalid Links found: {invalid_links}")
            logger.info(f"Invalid Links found: {invalid_links}")
            logger.info(f"Valid URNs from history: {valid_urns}")
        else:
            all_justifications.append("No invalid URNs found")

        judged_scores.append(1 if is_valid else 0)

    return mlflow_metrics.MetricValue(
        scores=all_scores,  # type: ignore
        justifications=all_justifications,
        aggregate_results={
            "pass_percentage": (
                100 * sum(judged_scores) / len(judged_scores)
                if len(judged_scores) > 0
                else 0
            ),
            "count": len(judged_scores),
        },
    )


def run_tool_eval_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> mlflow_metrics.MetricValue:
    """Evaluate tool call pass rates from chat history."""
    all_scores: List[float] = []
    all_justifications: List[str] = []
    judged_scores: List[float] = []

    # Aggregate tool statistics across all entries
    tool_stats = {}  # tool_name -> {"passed": int, "total": int}

    for prediction, _target in zip(predictions, targets, strict=False):
        if prediction is None or prediction == "" or prediction == "None":
            # Skip entries with no response
            all_scores.append(1)
            all_justifications.append("No chat history to evaluate")
            continue

        try:
            history = ChatHistory.model_validate_json(prediction)
            tool_calls = {"passed": 0, "total": 0}
            entry_tool_stats = {}

            for message in history.messages:
                if isinstance(message, (ToolResult, ToolResultError)):
                    tool_name = message.tool_request.tool_name
                    is_success = isinstance(message, ToolResult)

                    # Update entry-level stats
                    tool_calls["total"] += 1
                    if is_success:
                        tool_calls["passed"] += 1

                    # Update per-tool stats for this entry
                    if tool_name not in entry_tool_stats:
                        entry_tool_stats[tool_name] = {"passed": 0, "total": 0}
                    entry_tool_stats[tool_name]["total"] += 1
                    if is_success:
                        entry_tool_stats[tool_name]["passed"] += 1

                    # Update global tool stats
                    if tool_name not in tool_stats:
                        tool_stats[tool_name] = {"passed": 0, "total": 0}
                    tool_stats[tool_name]["total"] += 1
                    if is_success:
                        tool_stats[tool_name]["passed"] += 1

            # Calculate pass rate for this entry
            if tool_calls["total"] == 0:
                entry_pass_rate = 1.0  # No tool calls means 100% pass rate
                justification = "No tool calls found"
            else:
                entry_pass_rate = tool_calls["passed"] / tool_calls["total"]
                tool_breakdown = ", ".join(
                    [
                        f"{tool}: {stats['passed']}/{stats['total']}"
                        for tool, stats in entry_tool_stats.items()
                    ]
                )
                justification = f"Tools: {tool_breakdown}"

            all_scores.append(entry_pass_rate)
            all_justifications.append(justification)
            judged_scores.append(entry_pass_rate)

        except Exception as e:
            logger.warning(f"Error parsing chat history for tool evaluation: {e}")
            all_scores.append(0)
            all_justifications.append(f"Error parsing history: {str(e)}")

    # Calculate aggregate results
    aggregate_results = {
        "pass_percentage": (
            100 * sum(judged_scores) / len(judged_scores)
            if len(judged_scores) > 0
            else 0
        ),
        "count": len(judged_scores),
    }

    # Add per-tool pass rates to aggregate results
    for tool_name, stats in tool_stats.items():
        pass_rate = stats["passed"] / stats["total"] if stats["total"] > 0 else 0
        aggregate_results[f"{tool_name}/pass_percentage"] = 100 * pass_rate
        aggregate_results[f"{tool_name}/count"] = stats["total"]

    return mlflow_metrics.MetricValue(
        scores=all_scores,  # type: ignore
        justifications=all_justifications,
        aggregate_results=aggregate_results,
    )


def context_reduction_applied_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> mlflow_metrics.MetricValue:
    """Measure how often context reduction is applied across conversations."""
    all_scores: List[float] = []
    all_justifications: List[str] = []
    reduced_conversations = 0
    reducer_type_counts: CounterType[str] = Counter()
    reducer_applications: List[int] = []

    for prediction in predictions:
        if not prediction:
            all_scores.append(0.0)
            all_justifications.append("No chat history to evaluate")
            continue

        history = ChatHistory.model_validate_json(prediction)
        num_reducers = history.num_reducers_applied

        if num_reducers > 0:
            reduced_conversations += 1
            reducer_applications.append(num_reducers)
            all_scores.append(1.0)
            all_justifications.append(
                f"Context reduction applied: {num_reducers} reducers"
            )

            # Track reducer types from metadata
            for reducer_metadata in history.extra_properties.get("reducers", []):
                reducer_type_counts[
                    reducer_metadata.get("reducer_name", "unknown")
                ] += 1
        else:
            all_scores.append(0.0)
            all_justifications.append("No context reduction needed")

    # Build aggregate results
    aggregate_results = {
        "num_reduced_conversations": float(reduced_conversations),
        "typical_num_reducers": (
            float(np.median(reducer_applications)) if reducer_applications else 0.0
        ),
    }

    # Add reducer type counts
    for reducer_type, count in reducer_type_counts.items():
        aggregate_results[f"total_{reducer_type}_calls"] = float(count)

    return mlflow_metrics.MetricValue(
        scores=all_scores,
        justifications=all_justifications,
        aggregate_results=aggregate_results,
    )
