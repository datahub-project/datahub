from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import fnmatch
import pathlib
import time
from typing import Annotated, List, Optional

import asyncer
import mlflow
import pandas as pd
import typer
from anyio import to_thread
from datahub.sdk.main_client import DataHubClient
from loguru import logger
from mlflow import metrics as mlflow_metrics

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    HumanMessage,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.chat.chat_session import (
    CHATBOT_MODEL,
    ChatSession,
    NextMessage,
)
from datahub_integrations.experimentation.chatbot import (
    Prompt,
    prompts as all_prompts,
    prompts_file,
)
from datahub_integrations.experimentation.creds import create_uncached_datahub_graph
from datahub_integrations.experimentation.link_eval import (
    extract_datahub_links_from_response,
    extract_urns_from_history,
)
from datahub_integrations.experimentation.utils import execute_notebook_save_as_html
from datahub_integrations.gen_ai.description_v3 import ANYIO_THREAD_COUNT
from datahub_integrations.mcp.mcp_server import mcp

assert AI_EXPERIMENTATION_INITIALIZED

experiments_dir = pathlib.Path(__file__).parent


@mlflow.trace
async def run_prompt(case: Prompt, local_results_dir: pathlib.Path) -> dict:
    mlflow.update_current_trace(tags={"prompt_id": case.id})
    # span = mlflow.get_current_active_span()
    # assert span is not None
    # span.set_attribute("prompt_id", case.id)

    graph = create_uncached_datahub_graph(key=case.instance)
    client = DataHubClient(graph=graph)
    try:
        history = ChatHistory(messages=[HumanMessage(text=case.message)])
        session = ChatSession(tools=[mcp], client=client, history=history)

        next_message: NextMessage = await asyncer.asyncify(
            session.generate_next_message
        )()

        output_file = local_results_dir / f"{case.id}.json"
        history.save_file(output_file)
        mlflow.log_artifact(str(output_file), artifact_path="history")

        return {
            "response": next_message.text,
            "follow_up_suggestions": next_message.suggestions,
            "next_message": next_message.model_dump_json(),
            "history": history.model_dump_json(),
            "error": None,
        }
    except Exception as e:
        return {
            "response": None,
            "follow_up_suggestions": None,
            "next_message": None,
            "history": None,
            "error": str(e),
        }


def _has_response_metric_fn(predictions, targets, metrics):
    scores = [float(pred is not None and pred != "") for pred in predictions]
    return mlflow_metrics.MetricValue(
        scores=scores,
        aggregate_results={
            "pass_percentage": 100
            * len(list(filter(lambda x: x, scores)))
            / len(scores)
            if len(scores) > 0
            else 0,
            "count": len(scores),
        },
    )


has_response_metric = mlflow_metrics.make_metric(
    eval_fn=_has_response_metric_fn,
    name="has_response",
    greater_is_better=True,
)


# TODO: also validate instance name and entity type in entity link
def _has_valid_links_metric_fn(predictions, targets):
    """Check if all DataHub links in response reference valid URNs from tool results."""
    all_scores: List[Optional[bool]] = []
    all_justifications: List[Optional[str]] = []
    judged_scores: List[bool] = []

    for prediction, target in zip(predictions, targets, strict=False):
        if prediction is None or prediction == "" or prediction == "None":
            # Skip entries with no response
            all_scores.append(None)
            all_justifications.append("No response to evaluate")
            continue

        history = ChatHistory.model_validate_json(target)

        # Extract valid URNs from chat history
        valid_urns = extract_urns_from_history(history)

        # Extract DataHub links from response
        response_urns = extract_datahub_links_from_response(prediction)
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
            all_justifications.append(None)

        judged_scores.append(is_valid)

    return mlflow_metrics.MetricValue(
        scores=all_scores,
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


has_valid_links_metric = mlflow_metrics.make_metric(
    eval_fn=_has_valid_links_metric_fn,
    name="has_valid_links",
    greater_is_better=True,
)


def _run_tool_eval_metric_fn(predictions, targets):
    """Evaluate tool call pass rates from chat history."""
    all_scores: List[Optional[float]] = []
    all_justifications: List[Optional[str]] = []
    judged_scores: List[float] = []

    # Aggregate tool statistics across all entries
    tool_stats = {}  # tool_name -> {"passed": int, "total": int}

    for prediction, target in zip(predictions, targets, strict=False):
        if prediction is None or prediction == "" or prediction == "None":
            # Skip entries with no response
            all_scores.append(None)
            all_justifications.append("No response to evaluate")
            continue

        if not target:
            all_scores.append(None)
            all_justifications.append("No chat history to evaluate")
            continue

        try:
            history = ChatHistory.model_validate_json(target)
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
            all_scores.append(None)
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
        scores=all_scores,
        justifications=all_justifications,
        aggregate_results=aggregate_results,
    )


run_tool_eval_metric = mlflow_metrics.make_metric(
    eval_fn=_run_tool_eval_metric_fn,
    name="run_tool",
    greater_is_better=True,
)


async def main(prompt_ids: Annotated[Optional[List[str]], typer.Option(None)] = None):
    filtered_prompts = all_prompts
    if prompt_ids:
        filtered_prompts = [
            p
            for p in all_prompts
            if any(fnmatch.fnmatch(p.id, pattern) for pattern in prompt_ids)
        ]
        if len(filtered_prompts) == 0:
            logger.error(f"No prompts found for {prompt_ids}")
            return
        logger.info(f"Running {len(filtered_prompts)} prompts")

    to_thread.current_default_thread_limiter().total_tokens = ANYIO_THREAD_COUNT
    with mlflow.start_run() as run:
        assert run.info.run_name is not None
        experiment_results_dir: pathlib.Path = (
            experiments_dir / "runs" / run.info.run_name
        )
        experiment_results_dir.mkdir(parents=True)

        mlflow.log_artifact(str(prompts_file))
        mlflow.log_params(
            {
                "model": CHATBOT_MODEL,
                "anyio_thread_count": ANYIO_THREAD_COUNT,
            }
        )

        tasks: list[tuple[Prompt, asyncer.SoonValue[dict]]] = []
        async with asyncer.create_task_group() as tg:
            for prompt in filtered_prompts:
                tasks.append(
                    (
                        prompt,
                        tg.soonify(run_prompt)(
                            prompt, local_results_dir=experiment_results_dir
                        ),
                    )
                )

        logger.info("All tasks finished")

        results = []
        for prompt, task in tasks:
            results.append(
                {
                    "prompt_id": prompt.id,
                    **prompt.model_dump(exclude={"id"}),
                    **task.value,
                }
            )
        results_df = pd.DataFrame(results)

        logger.info("Evaluating results")
        eval_result = mlflow.evaluate(
            data=results_df,
            predictions="response",
            evaluators="default",
            targets="history",
            extra_metrics=[
                has_response_metric,
                has_valid_links_metric,
                run_tool_eval_metric,
                # mlflow.metrics.token_count(),
            ],
        )
        logger.debug(eval_result)
        try:
            html_path = execute_notebook_save_as_html(
                experiments_dir / "analyze_run.ipynb",
                pathlib.Path(experiment_results_dir),
                {"RUN_NAME": run.info.run_name},
            )
            mlflow.log_artifact(html_path)
        except Exception as e:
            logger.error(f"Error executing notebook: {e}")


if __name__ == "__main__":
    typer.run(asyncer.syncify(main, raise_sync_error=False))
    logger.info("Sleeping for 5 seconds to ensure logging is flushed")
    time.sleep(5)
