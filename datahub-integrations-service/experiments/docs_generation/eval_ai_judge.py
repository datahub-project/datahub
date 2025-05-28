import datetime
import os
import tempfile
from typing import Dict, List, Tuple

import dotenv
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import seaborn as sns
import typer
from datahub.ingestion.api.report_helpers import format_datetime_relative
from eval_common import (
    METRIC_NAMES,
    get_ai_annotation_run_name,
    get_ai_judge_eval_run_name,
    get_human_annotation_run_name,
)
from mlflow_common import (
    get_ai_eval_result_or_none,
    get_human_eval_result_or_none,
    get_run_or_fail,
)
from pydantic import BaseModel, Field
from run_ai_annotations import run_ai_annotations_experiment

EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME")
mlflow.set_experiment(EXPERIMENT_NAME)
mlflow.bedrock.autolog()
dotenv.load_dotenv()


class MetricEvaluation(BaseModel):
    """Evaluation results for a single metric."""

    class Config:
        arbitrary_types_allowed = True

    metric: str
    matches: List[str] = Field(
        description="List of URNs where human and AI scores match"
    )
    mismatches: List[str] = Field(
        description="List of URNs where human and AI scores differ"
    )
    missing: List[str] = Field(
        description="List of URNs present in human but not in AI evaluations"
    )
    confusion_matrix: pd.DataFrame = Field(
        description="Confusion matrix of human vs AI scores"
    )
    misclassification_analysis: pd.DataFrame = Field(
        description="Detailed analysis of mismatched cases"
    )
    match_percentage: float = Field(description="Percentage of matching scores")
    total: int = Field(description="Total number of evaluations")
    confusion_matrix_path: str = Field(description="Path to confusion matrix plot")


class EvaluationOutput(BaseModel):
    """Complete output of AI judge evaluation."""

    class Config:
        arbitrary_types_allowed = True

    summary_df: pd.DataFrame = Field(description="Summary statistics for all metrics")
    metric_evaluations: Dict[str, MetricEvaluation] = Field(
        description="Detailed evaluation per metric"
    )


def format_time(time: float) -> str:
    return format_datetime_relative(datetime.datetime.fromtimestamp(time / 1000))


def create_confusion_matrix(
    human_scores: pd.Series, ai_scores: pd.Series, metric_name: str
) -> Tuple[pd.DataFrame, str]:
    """Create and save confusion matrix plot for a given metric.

    Args:
        human_scores: Series of human evaluation scores
        ai_scores: Series of AI evaluation scores
        metric_name: Name of the metric being evaluated

    Returns:
        Tuple of (confusion matrix DataFrame, path to saved plot)
    """
    cm = pd.crosstab(human_scores, ai_scores, rownames=["Human"], colnames=["AI"])

    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
    plt.title(f"Confusion Matrix - {metric_name}")

    with tempfile.NamedTemporaryFile(
        suffix=f"_{metric_name}_confusion_matrix.png", delete=False
    ) as tmp:
        plt.savefig(tmp.name)
        plt.close()
        return cm, tmp.name


def get_valid_scores(
    human_eval_results: pd.DataFrame, ai_eval_results: pd.DataFrame, score_col: str
) -> Tuple[pd.Series, pd.Series]:
    """Get valid scores from both human and AI evaluations.

    Args:
        human_eval_results: DataFrame containing human evaluation results
        ai_eval_results: DataFrame containing AI evaluation results
        score_col: Name of the score columnW

    Returns:
        Tuple of (valid human scores, valid AI scores) for common URNs
    """
    common_urns = human_eval_results.index.intersection(ai_eval_results.index)
    valid_human_scores = human_eval_results.loc[common_urns][score_col].dropna()
    valid_ai_scores = ai_eval_results.loc[valid_human_scores.index][score_col].dropna()

    common_valid_urns = valid_human_scores.index.intersection(valid_ai_scores.index)
    return (
        valid_human_scores.loc[common_valid_urns],
        valid_ai_scores.loc[common_valid_urns],
    )


def create_misclassification_analysis(
    human_eval_results: pd.DataFrame,
    ai_eval_results: pd.DataFrame,
    matches: pd.Series,
    valid_human_scores: pd.Series,
    valid_ai_scores: pd.Series,
    reason_col: str,
) -> pd.DataFrame:
    """Create detailed analysis of misclassified cases.

    Args:
        human_eval_results: DataFrame containing human evaluation results
        ai_eval_results: DataFrame containing AI evaluation results
        matches: Boolean series indicating matches
        valid_human_scores: Series of valid human scores
        valid_ai_scores: Series of valid AI scores
        reason_col: Name of the reason/justification column

    Returns:
        DataFrame containing misclassification analysis
    """
    return pd.DataFrame(
        {
            "urn": matches[~matches].index,
            "description": human_eval_results.loc[matches[~matches].index][
                "description"
            ].values,
            "human_score": valid_human_scores[~matches],
            "ai_score": valid_ai_scores[~matches],
            "human_reason": human_eval_results.loc[matches[~matches].index][
                reason_col
            ].values,
            "ai_reason": ai_eval_results.loc[matches[~matches].index][
                reason_col
            ].values,
        }
    )


def evaluate_metric(
    human_eval_results: pd.DataFrame, ai_eval_results: pd.DataFrame, metric_name: str
) -> MetricEvaluation:
    """Evaluate a single metric by comparing human and AI scores.

    Args:
        human_eval_results: DataFrame containing human evaluation results
        ai_eval_results: DataFrame containing AI evaluation results
        metric_name: Name of the metric to evaluate

    Returns:
        MetricEvaluation containing all evaluation results for the metric
    """
    score_col = f"{metric_name}/score"
    reason_col = f"{metric_name}/justification"

    # Get valid scores for comparison
    valid_human_scores, valid_ai_scores = get_valid_scores(
        human_eval_results, ai_eval_results, score_col
    )

    # Create confusion matrix
    cm, plot_path = create_confusion_matrix(
        valid_human_scores, valid_ai_scores, metric_name
    )

    # Calculate matches and mismatches
    matches = valid_human_scores == valid_ai_scores

    # Create misclassification analysis
    misclass_analysis = create_misclassification_analysis(
        human_eval_results,
        ai_eval_results,
        matches,
        valid_human_scores,
        valid_ai_scores,
        reason_col,
    )

    # Calculate metrics
    len_matches = len(matches[matches])
    len_total = len(matches)

    return MetricEvaluation(
        metric=metric_name,
        matches=matches[matches].index.tolist(),
        mismatches=matches[~matches].index.tolist(),
        missing=human_eval_results.index.difference(ai_eval_results.index).tolist(),
        confusion_matrix=cm,
        misclassification_analysis=misclass_analysis,
        match_percentage=(100 * len_matches / len_total) if len_total > 0 else 0,
        total=len_total,
        confusion_matrix_path=plot_path,
    )


def eval_ai_judge_accuracy(
    ai_eval_results: pd.DataFrame, human_eval_results: pd.DataFrame
) -> EvaluationOutput:
    """Evaluate AI judge accuracy by comparing with human annotations.

    Args:
        ai_eval_results: DataFrame containing AI evaluation results
        human_eval_results: DataFrame containing human evaluation results

    Returns:
        EvaluationOutput containing all evaluation results and artifacts
    """
    # set index to urn
    ai_eval_results = ai_eval_results.set_index("urn")
    human_eval_results = human_eval_results.set_index("urn")

    # Evaluate each metric
    metric_evaluations: Dict[str, MetricEvaluation] = {}

    for metric_name in METRIC_NAMES:
        evaluation = evaluate_metric(human_eval_results, ai_eval_results, metric_name)
        metric_evaluations[metric_name] = evaluation

    # Create summary DataFrame
    summary_df = pd.DataFrame(
        [
            {
                "metric": eval.metric,
                "matches": len(eval.matches),
                "mismatches": len(eval.mismatches),
                "missing": len(eval.missing),
                "total": eval.total,
                "match_percentage": eval.match_percentage,
            }
            for eval in metric_evaluations.values()
        ]
    )

    return EvaluationOutput(
        summary_df=summary_df,
        metric_evaluations=metric_evaluations,
    )


def run_eval_ai_judge_experiment(run_name: str, existing_run: bool = True) -> None:
    """Run AI judge evaluation experiment.

    Args:
        run_name: Name of the run to evaluate
        existing_run: Whether to use existing run or create new one
    """
    human_annotation_run = get_run_or_fail(get_human_annotation_run_name(run_name))
    human_eval_results = get_human_eval_result_or_none(run_name)
    assert human_eval_results is not None

    if not existing_run:
        run_ai_annotations_experiment(run_name)

    ai_run = get_run_or_fail(get_ai_annotation_run_name(run_name))
    ai_eval_results = get_ai_eval_result_or_none(run_name)
    assert ai_eval_results is not None

    with mlflow.start_run(
        experiment_id=human_annotation_run.info.experiment_id,
        run_name=get_ai_judge_eval_run_name(run_name),
    ):
        mlflow.set_tag("evaluation_type", "ai_judge_eval")
        mlflow.log_params(
            {
                "human_eval_run_id": human_annotation_run.info.run_id,
                "ai_eval_run_id": ai_run.info.run_id,
                **ai_run.data.params,
            }
        )

        # Log the eval_ai_judge.py script
        mlflow.log_artifact("./eval_ai_judge.py")

        # Run evaluation
        evaluation_output = eval_ai_judge_accuracy(ai_eval_results, human_eval_results)

        # Log results to MLflow
        mlflow.log_table(evaluation_output.summary_df, "ai_judge_accuracy.json")

        # Log misclassification analysis for each metric
        for metric_name, evaluation in evaluation_output.metric_evaluations.items():
            mlflow.log_table(
                evaluation.misclassification_analysis,
                f"misclassification_analyses/{metric_name}.json",
            )
            mlflow.log_artifact(evaluation.confusion_matrix_path, "confusion_matrices")
            os.unlink(evaluation.confusion_matrix_path)

        # Log metrics
        eval_df_idx = evaluation_output.summary_df.set_index("metric")
        for metric_name in METRIC_NAMES:
            mlflow.log_metric(
                f"{metric_name}_match_percentage",
                eval_df_idx.loc[metric_name]["match_percentage"],
            )


if __name__ == "__main__":
    typer.run(run_eval_ai_judge_experiment)
