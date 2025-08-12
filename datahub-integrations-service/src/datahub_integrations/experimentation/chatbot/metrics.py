from typing import List

import mlflow.metrics as mlflow_metrics
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
    extract_datahub_links_from_response,
    extract_response_from_history_json,
    extract_urns_from_history,
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
    all_scores: List[bool] = []
    all_justifications: List[str] = []
    judged_scores: List[bool] = []

    for prediction, target in zip(predictions, targets, strict=False):
        prompt = Prompt.model_validate_json(target)
        # Skip if no expected_tool_calls defined in target
        if prompt.expected_tool_calls is None:
            all_scores.append(False)
            all_justifications.append("No expected tool calls defined for this prompt")
            continue

        if prediction is None or prediction == "" or prediction == "None":
            all_scores.append(False)
            all_justifications.append("No chat history to evaluate")
            continue

        try:
            # prediction contains the chat history JSON string
            history = ChatHistory.model_validate_json(prediction)

            # Validate expected tool calls
            validation_result = validate_expected_tool_calls(
                history, prompt.expected_tool_calls
            )

            all_scores.append(validation_result.is_valid)
            all_justifications.append(validation_result.justification)
            judged_scores.append(validation_result.is_valid)

        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.warning(f"Error validating expected tool calls: {e}")
            all_scores.append(False)
            all_justifications.append(f"Error validating tool calls: {str(e)}")

    return mlflow_metrics.MetricValue(
        scores=all_scores,  # type: ignore
        justifications=all_justifications,
        aggregate_results={
            "pass_percentage": (
                100 * len([s for s in judged_scores if s]) / len(judged_scores)
                if len(judged_scores) > 0
                else 0
            ),
            "count": len(judged_scores),
        },
    )


# TODO: also validate instance name and entity type in entity link
def has_valid_links_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> mlflow_metrics.MetricValue:
    """Check if all DataHub links in response reference valid URNs from tool results."""
    all_scores: List[bool] = []
    all_justifications: List[str] = []
    judged_scores: List[bool] = []

    for prediction, _target in zip(predictions, targets, strict=False):
        # Extract response from chat history
        response = extract_response_from_history_json(prediction)

        if not response:
            # Skip entries with no response
            all_scores.append(False)
            all_justifications.append("No response to evaluate")
            continue

        history = ChatHistory.model_validate_json(prediction)

        # Extract valid URNs from chat history
        valid_urns = extract_urns_from_history(history)

        # Extract DataHub links from response
        # TODO: extract other links . e.g. the URN only links [table_name](urn:li:dataset:...), this is invalid
        response_urns = extract_datahub_links_from_response(response)
        logger.debug(f"Response URNs: {response_urns}")

        # Check if all response URNs are valid
        invalid_urns = list(set(response_urns) - set(valid_urns))

        is_valid = len(invalid_urns) == 0
        all_scores.append(is_valid)

        if not is_valid:
            all_justifications.append(f"Invalid URNs found: {invalid_urns}")
            logger.info(f"Invalid URNs found: {invalid_urns}")
            logger.info(f"Valid URNs from history: {valid_urns}")
        else:
            all_justifications.append("No invalid URNs found")

        judged_scores.append(is_valid)

    return mlflow_metrics.MetricValue(
        scores=all_scores,  # type: ignore
        justifications=all_justifications,
        aggregate_results={
            "pass_percentage": (
                100 * len(list(filter(lambda x: x, judged_scores))) / len(judged_scores)
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
            all_scores.append(False)
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
            all_scores.append(False)
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
