from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import json
from typing import List, Optional

import asyncer
import mlflow
import pandas as pd
import typer
from loguru import logger
from mlflow.entities import Run
from mlflow.metrics import MetricValue

from datahub_integrations.experimentation.judge import (
    CHATBOT_AI_JUDGE_MODEL,
    BedrockModel,
    LLMJudgeResponse,
    chatbot_llm_judge_evaluation,
)

assert AI_EXPERIMENTATION_INITIALIZED

# Chatbot experiment constants
EXPERIMENT_NAME = "Chatbot"
mlflow.set_experiment(EXPERIMENT_NAME)

# AI judge model - using same as docs generation
AI_JUDGE_MODEL = BedrockModel.CLAUDE_4_SONNET


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
    return f"ai_eval_{run_name}"


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


def eval_single_guideline_adherence(
    prediction: str, target: str
) -> Optional[LLMJudgeResponse]:
    """Evaluate a single prediction against guidelines."""
    if prediction is None or prediction == "" or prediction == "None":
        return None

    try:
        target_dict = json.loads(target) if isinstance(target, str) else target
        message = target_dict.get("message", "")
        response_guidelines = target_dict.get("response_guidelines", "")

        if not response_guidelines:
            return None

        judge_response: LLMJudgeResponse = chatbot_llm_judge_evaluation(
            message=message,
            response=prediction,
            guidelines=response_guidelines,
        )
        return judge_response

    except Exception as e:
        logger.warning(f"Error evaluating guideline adherence: {e}")
        return None


def _guideline_adherence_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> MetricValue:
    """Evaluate responses against guidelines using LLM judge."""
    all_scores: List[Optional[bool]] = [None] * len(predictions)
    all_justifications: List[Optional[str]] = [None] * len(predictions)
    judged_scores: List[bool] = []

    judge_responses = asyncer.syncify(eval_guideline_adherence, raise_sync_error=False)(
        predictions, targets
    )

    for i, judge_response in enumerate(judge_responses):
        if judge_response is None:
            # Handle cases where evaluation was skipped or failed
            if (
                predictions.iloc[i] is None
                or predictions.iloc[i] == ""
                or predictions.iloc[i] == "None"
            ):
                all_justifications[i] = "No response to evaluate"
            else:
                try:
                    target_dict = (
                        json.loads(targets.iloc[i])
                        if isinstance(targets.iloc[i], str)
                        else targets.iloc[i]
                    )
                    response_guidelines = target_dict.get("response_guidelines", "")
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


# Create custom metrics
guideline_adherence_metric = mlflow.metrics.make_metric(
    eval_fn=_guideline_adherence_metric_fn,
    greater_is_better=True,
    name="guideline_adherence",
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
    def create_target_dict(row):
        return {
            "message": row["message"],
            "response_guidelines": row["response_guidelines"],
            "prompt_id": row["prompt_id"],
            "instance": row["instance"],
        }

    results_df["target"] = results_df.apply(create_target_dict, axis=1)

    ai_eval_run_name = get_ai_eval_run_name(run_name)

    with mlflow.start_run(
        experiment_id=run.info.experiment_id,
        run_name=ai_eval_run_name,
        description=run_description,
    ):
        # Log current file as artifact
        mlflow.log_artifact(__file__)
        mlflow.set_tag("evaluation_type", "ai_judge")

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
                predictions="response",
                evaluators="default",
                targets="target",
                extra_metrics=[
                    guideline_adherence_metric,
                ],
            )
            logger.info("AI evaluation completed successfully")
        except Exception as e:
            logger.error(f"Error during AI evaluation: {e}")
            raise


def main(run_name: str):
    """Main entry point that accepts run name as CLI argument."""
    logger.info(f"Starting AI evaluation for run: {run_name}")
    run_ai_evaluation(run_name)


if __name__ == "__main__":
    typer.run(main)
