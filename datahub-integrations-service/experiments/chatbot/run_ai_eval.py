from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import os
import platform
import socket
from typing import List, Optional

import asyncer
from datahub_integrations.gen_ai.model_config import BedrockModel
import mlflow
import mlflow.metrics
import pandas as pd
import typer
from loguru import logger
from mlflow.entities import Run
from mlflow.metrics import MetricValue

from datahub_integrations.experimentation.chatbot.chatbot import (
    Prompt,
    prompts,
)
from datahub_integrations.experimentation.chatbot.eval_helpers import (
    extract_response_from_history_json,
)
from datahub_integrations.experimentation.chatbot.judge import (
    CHATBOT_AI_JUDGE_MODEL,
    LLMJudgeResponse,
    chatbot_llm_judge_evaluation,
)
from datahub_integrations.experimentation.chatbot.metrics import (
    context_reduction_applied_metric_fn,
    expected_tool_calls_metric_fn,
    has_valid_links_metric_fn,
    run_tool_eval_metric_fn,
)
from datahub_integrations.experimentation.mlflow_utils import (
    get_most_recent_run_for_expt,
)

assert AI_EXPERIMENTATION_INITIALIZED

# Chatbot experiment constants
EXPERIMENT_NAME = "Chatbot"
mlflow.set_experiment(EXPERIMENT_NAME)

# AI judge model - using same as docs generation
AI_JUDGE_MODEL = BedrockModel.CLAUDE_4_SONNET
prompt_map = {p.id: p for p in prompts}


def get_run_or_fail(run_name: str) -> Run:
    """Get MLflow run or fail if not found."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}'",
        output_format="list",
        order_by=["start_time DESC"],
    )
    if not runs:
        raise ValueError(f"Run {run_name} not found")
    return runs[0]


def get_ai_eval_run_name(run_name: str) -> str:
    """Generate AI evaluation run name with prefix."""
    original_run_name = run_name.replace("ai_eval_", "")
    return f"ai_eval_{original_run_name}"


async def eval_guideline_adherence(
    predictions: pd.Series, targets: pd.Series
) -> List[Optional[LLMJudgeResponse]]:
    """Async function to evaluate guideline adherence."""
    results: List[asyncer.SoonValue[Optional[LLMJudgeResponse]]] = []

    async with asyncer.create_task_group() as task_group:
        for prediction, target in zip(predictions, targets, strict=False):
            result = task_group.soonify(
                asyncer.asyncify(eval_single_guideline_adherence)
            )(prediction, target)
            results.append(result)  # type:ignore

    return [result.value for result in results]


@mlflow.trace
def eval_single_guideline_adherence(
    prediction: str, target: str
) -> Optional[LLMJudgeResponse]:
    """Evaluate a single prediction against guidelines."""
    if prediction is None or prediction == "" or prediction == "None":
        return None

    try:
        prompt = Prompt.model_validate_json(target)
        message = prompt.message
        response_guidelines = prompt.response_guidelines
        mlflow.update_current_trace(tags={"prompt_id": prompt.id})

        if not message or not response_guidelines:
            return None

        judge_response: LLMJudgeResponse = chatbot_llm_judge_evaluation(
            message=message,
            response=prediction,
            guidelines=response_guidelines or "",
        )
        return judge_response

    except Exception as e:
        logger.warning(f"Error evaluating guideline adherence: {e}")
        return None


def guideline_adherence_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> MetricValue:
    """Evaluate responses against guidelines using LLM judge."""
    responses = [extract_response_from_history_json(pred) for pred in predictions]

    all_scores: List[Optional[bool]] = [None] * len(responses)
    all_justifications: List[Optional[str]] = [None] * len(responses)
    judged_scores: List[bool] = []

    judge_responses = asyncer.syncify(eval_guideline_adherence, raise_sync_error=False)(
        responses, targets
    )

    for i, judge_response in enumerate(judge_responses):
        if judge_response is None:
            # Handle cases where evaluation was skipped or failed
            if responses[i] is None or responses[i] == "" or responses[i] == "None":
                all_justifications[i] = "No response to evaluate"
            else:
                try:
                    prompt = Prompt.model_validate_json(targets.iloc[i])
                    response_guidelines = prompt.response_guidelines
                    if not response_guidelines:
                        all_justifications[i] = "No guidelines provided"
                    else:
                        all_justifications[i] = "Evaluation error"
                except Exception:
                    all_justifications[i] = "Evaluation error"
        else:
            all_scores[i] = judge_response.choice
            all_justifications[i] = judge_response.justification
            if judge_response.choice is not None:
                judged_scores.append(judge_response.choice)

    return MetricValue(
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


def model_fn(x: pd.DataFrame) -> pd.DataFrame:
    """Dummy model function required by mlflow.evaluate."""
    return x


def run_ai_evaluation(run_name: str, run_description: Optional[str] = None) -> None:
    """Run AI evaluation on a completed chatbot experiment."""
    run: Run = get_run_or_fail(run_name)

    # Load the experiment results
    artifact_path = "eval_results_table.json"
    results_df = mlflow.load_table(
        artifact_file=artifact_path, run_ids=[run.info.run_id]
    )

    # Hack to get multiple columns from source dataframe
    # Alternatively, we can evaluate metrics outside mlflow.evaluate and just log them
    # Create target column combining message and response_guidelines for evaluation
    # Using prompts from file to pick latest updated response_guidelines and expected_tool_calls
    def create_target_column(row):
        if row["prompt_id"] not in prompt_map:
            raise ValueError(f"Prompt {row['prompt_id']} not found in prompt map")
        prompt = prompt_map[row["prompt_id"]]
        return prompt.model_dump_json()

    results_df["target"] = results_df.apply(create_target_column, axis=1)

    ai_eval_run_name = get_ai_eval_run_name(run_name)

    with mlflow.start_run(
        experiment_id=run.info.experiment_id,
        run_name=ai_eval_run_name,
        description=run_description,
    ):
        # Log current file as artifact
        mlflow.log_artifact(__file__)
        mlflow.set_tag("evaluation_type", "ai_judge")

        # Tag run with machine/environment metadata for debugging and tracking
        mlflow.set_tags({
            "machine.hostname": socket.gethostname(),
            "machine.user": os.getenv("USER") or os.getenv("USERNAME") or "unknown",
            "machine.os": f"{platform.system()} {platform.release()}",
            "machine.python_version": platform.python_version(),
        })

        # Log parameters including AI judge model and original run parameters
        mlflow.log_params(
            {
                "ai_judge_model": CHATBOT_AI_JUDGE_MODEL,
                **{f"original_{k}": v for k, v in run.data.params.items()},
            }
        )
        # Log original run metrics
        mlflow.log_metrics(
            {k: v for k, v in run.data.metrics.items()},
        )

        logger.info(f"Starting AI evaluation with {len(results_df)} entries")

        try:
            mlflow.evaluate(
                model=model_fn,
                data=results_df,
                predictions="history",
                evaluators="default",
                targets="target",
                extra_metrics=[
                    mlflow.metrics.make_metric(
                        eval_fn=has_valid_links_metric_fn,
                        name="has_valid_links",
                        greater_is_better=True,
                    ),
                    mlflow.metrics.make_metric(
                        eval_fn=run_tool_eval_metric_fn,
                        name="run_tool",
                        greater_is_better=True,
                    ),
                    mlflow.metrics.make_metric(
                        eval_fn=guideline_adherence_metric_fn,
                        greater_is_better=True,
                        name="guideline_adherence",
                    ),
                    mlflow.metrics.make_metric(
                        eval_fn=expected_tool_calls_metric_fn,
                        name="expected_tool_calls",
                        greater_is_better=True,
                    ),
                    mlflow.metrics.make_metric(
                        eval_fn=context_reduction_applied_metric_fn,
                        name="context_reducers",
                        greater_is_better=True,
                    ),
                ],
            )
            logger.info("AI evaluation completed successfully")
        except Exception as e:
            logger.error(f"Error during AI evaluation: {e}")
            raise


def main(run_name: Optional[str] = None):
    """Main entry point that accepts run name as CLI argument."""

    if run_name is None:
        run_name = get_most_recent_run_for_expt(EXPERIMENT_NAME).info.run_name
    logger.info(f"Starting AI evaluation for run: {run_name}")
    run_ai_evaluation(run_name)


if __name__ == "__main__":
    typer.run(main)
