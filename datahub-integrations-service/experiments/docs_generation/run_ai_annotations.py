from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import functools
from typing import Any, Dict, List, Optional, Union

import asyncer
import mlflow
import mlflow.bedrock
import mlflow.metrics
import numpy as np
import pandas as pd
import typer
from loguru import logger
from mlflow.entities import Run
from mlflow.metrics import MetricValue

from datahub_integrations.experimentation.docs_generation.eval_common import (
    METRICS_CONFIG,
    JudgedMetricValue,
    docs_generation_experiments_dir,
    get_ai_annotation_run_name,
    get_human_guidelines,
    get_overall_score,
    to_entity_info_model,
    to_entity_info_model_json,
)
from datahub_integrations.experimentation.docs_generation.llm_judge import (
    AI_JUDGE_MODEL,
    llm_judge_common_eval_fn,
)
from datahub_integrations.experimentation.docs_generation.metrics import (
    has_table_description_metric_fn,
    has_valid_links_metric_fn,
)
from datahub_integrations.experimentation.docs_generation.mlflow_common import (
    EXPERIMENT_NAME,
    get_run_or_fail,
)
from datahub_integrations.gen_ai.description_context import (
    ExtractedTableInfo,
)

assert AI_EXPERIMENTATION_INITIALIZED
mlflow.set_experiment(EXPERIMENT_NAME)


def custom_eval_fn_metric(
    metric: str, predictions: pd.Series, targets: pd.Series
) -> MetricValue:
    all_scores: List[Optional[bool]] = [None] * len(predictions)
    all_justifications: List[Optional[str]] = [None] * len(predictions)
    judged_scores: List[bool] = []
    metric_values = asyncer.syncify(eval_metrics, raise_sync_error=False)(
        metric, predictions, targets
    )
    for i, judged_metric in enumerate(metric_values):
        if judged_metric is not None:
            judged_value: Optional[bool] = judged_metric.bool_value()
            all_scores[i] = judged_value
            all_justifications[i] = judged_metric.reasoning
            if judged_value is not None:
                judged_scores.append(judged_value)
    return mlflow.metrics.MetricValue(
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


async def eval_metrics(
    metric: str, predictions: pd.Series, targets: pd.Series
) -> List[Optional[JudgedMetricValue]]:
    results: List[asyncer.SoonValue[JudgedMetricValue]] = []
    batch_size = 5

    # Process in batches of 5
    for i in range(0, len(predictions), batch_size):
        batch_predictions = predictions.iloc[i : i + batch_size]
        batch_targets = targets.iloc[i : i + batch_size]

        async with asyncer.create_task_group() as task_group:
            for prediction, target in zip(
                batch_predictions, batch_targets, strict=False
            ):
                result = task_group.soonify(
                    asyncer.asyncify(eval_metric_value_ai_judge)
                )(metric, prediction, target)
                results.append(result)  # type:ignore

    return [result.value for result in results]


def eval_metric_value_ai_judge(
    metric: str,
    prediction: str,
    target: Union[str, Dict[str, Any], ExtractedTableInfo],  # urn or entity info dict
) -> Optional[JudgedMetricValue]:
    human_guidelines = get_human_guidelines()
    human_guidelines_str: Optional[str] = None
    entity_info_model = to_entity_info_model(target)
    if entity_info_model.urn in human_guidelines:
        human_guidelines_str = human_guidelines[entity_info_model.urn].model_dump_json()
    else:
        human_guidelines_str = None
    judged_metric = getattr(
        llm_judge_common_eval_fn(
            prediction,
            to_entity_info_model_json(entity_info_model),
            human_guidelines_str,
        ),
        metric,
    )

    return judged_metric


def overall_score_eval_fn(predictions: pd.Series, targets: pd.Series) -> MetricValue:
    all_scores: List[Optional[int]] = []
    all_justifications: List[Optional[str]] = []
    judged_scores: List[int] = []
    human_guidelines = get_human_guidelines()
    for prediction, target in zip(predictions, targets, strict=False):
        entity_info_model = to_entity_info_model(target)
        table_human_guidelines_str: Optional[str] = None
        if entity_info_model.urn in human_guidelines:
            table_human_guidelines_str = human_guidelines[
                entity_info_model.urn
            ].model_dump_json()
        judged_metric = get_overall_score(
            llm_judge_common_eval_fn(
                prediction,
                to_entity_info_model_json(entity_info_model),
                table_human_guidelines_str,
            )
        )
        if judged_metric is not None:
            all_scores.append(judged_metric.value)
            all_justifications.append(judged_metric.reasoning)
            # this is hack to keep only judged entries. reasoning in always present.
            if judged_metric.reasoning is not None:
                assert isinstance(judged_metric.value, int)
                judged_scores.append(judged_metric.value)
        else:
            all_scores.append(0)
            all_justifications.append(None)

    return mlflow.metrics.MetricValue(
        scores=all_scores,
        justifications=all_justifications,
        aggregate_results={
            "mean": np.mean(judged_scores),
            "median": np.median(judged_scores),
            "count": len(judged_scores),
        },
    )


def make_overall_score_metric() -> MetricValue:
    return mlflow.metrics.make_metric(
        eval_fn=overall_score_eval_fn,
        greater_is_better=True,
        name="overall_score",
    )


def make_custom_metric(metric_name: str) -> MetricValue:
    """Mlflow custom AI metric allows generating single metric from single prompt.
    Since we need to generate multiple metrics from single prompt, we are using mlflow.metrics.make_metric
    with cache powered custom eval_fn that can generate multiple metrics from single prompt, instead of using
    mlflow.metrics.make_genai_metric_from_prompt that can only generate single metric from single prompt.
    """
    return mlflow.metrics.make_metric(
        eval_fn=functools.partial(custom_eval_fn_metric, metric_name),
        greater_is_better=True,
        name=metric_name,
    )


# TODO: consider non-llm metrics for this computation
metric_overall_score = make_overall_score_metric()

# TODO: Relog other column level metrics to get full picture at one place
ai_metrics = [
    *[make_custom_metric(metric.name) for metric in METRICS_CONFIG],
    metric_overall_score,
    mlflow.metrics.make_metric(
        eval_fn=has_table_description_metric_fn,
        name="has_table_description",
        greater_is_better=True,
    ),
    mlflow.metrics.make_metric(
        eval_fn=has_valid_links_metric_fn,
        name="has_valid_links",
        greater_is_better=True,
    ),
]


def model_fn(x: pd.DataFrame) -> pd.DataFrame:
    return x


def run_ai_annotations_experiment(
    run_name: str, run_description: Optional[str] = None
) -> None:
    run: Run = get_run_or_fail(run_name)

    artifact_path = "eval_results_table.json"
    table_descriptions = mlflow.load_table(
        artifact_file=artifact_path, run_ids=[run.info.run_id]
    )

    ai_annotation_run_name = get_ai_annotation_run_name(run_name)

    with mlflow.start_run(
        experiment_id=run.info.experiment_id,
        run_name=ai_annotation_run_name,
        description=run_description,
    ):
        # log current file as artifact
        mlflow.log_artifact(
            str(docs_generation_experiments_dir / "run_ai_annotations.py")
        )
        mlflow.log_artifact(
            str(
                docs_generation_experiments_dir / "eval_config/eval_set_guidelines.yaml"
            )
        )
        mlflow.set_tag("evaluation_type", "ai_judge")
        mlflow.log_params(
            {
                "ai_judge_model": AI_JUDGE_MODEL,
                **{f"prompt_expt_{k}": v for k, v in run.data.params.items()},
            }
        )

        try:
            mlflow.evaluate(
                model=model_fn,
                data=table_descriptions,
                predictions="description",
                evaluators="default",
                targets="entity_info",
                extra_metrics=[*ai_metrics],
            )
        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.error(f"Error evaluating metrics: {e}")
            breakpoint()


if __name__ == "__main__":
    typer.run(run_ai_annotations_experiment)
