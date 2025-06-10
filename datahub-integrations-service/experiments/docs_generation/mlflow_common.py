import functools
import os
from typing import Optional

import dotenv
import mlflow
import pandas as pd
from eval_common import (
    METRIC_NAMES,
    get_ai_annotation_run_name,
    get_human_annotation_run_name,
)
from loguru import logger
from mlflow.entities import Run

dotenv.load_dotenv()
EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME", "docs_generation")
mlflow.set_experiment(EXPERIMENT_NAME)


def get_run_or_fail(run_name: str, additional_filter_string: str = "") -> Run:
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}' {additional_filter_string}",
        output_format="list",
        order_by=["start_time DESC"],
    )
    if not runs:
        raise ValueError(f"No runs found for {run_name}")
    return runs[0]


def get_human_evals(run_id: str) -> pd.DataFrame:
    table = load_eval_table(run_id)
    return keep_human_evals_only(table)


@functools.cache
def load_eval_table(run_id: str) -> pd.DataFrame:
    artifact_path = "eval_results_table.json"
    table = mlflow.load_table(
        artifact_file=artifact_path,
        run_ids=[run_id],
    )

    return table


def keep_human_evals_only(table: pd.DataFrame) -> pd.DataFrame:
    return table[
        pd.concat(
            [table[f"{metric_name}/score"].notna() for metric_name in METRIC_NAMES],
            axis=1,
        ).any(axis=1)
    ]


def get_ai_eval_result_or_none(run_name: str) -> Optional[pd.DataFrame]:
    ai_run_name = get_ai_annotation_run_name(run_name)
    try:
        ai_run = get_run_or_fail(
            ai_run_name,
            'and tags.evaluation_type!="eval_ai_judge" and tags.evaluation_type!="ai_judge_eval" and attributes.status = "FINISHED"',
        )
        logger.info(f"AI run id: {ai_run.info.run_id}")

        artifact_path = "eval_results_table.json"

        table = mlflow.load_table(
            artifact_file=artifact_path,
            run_ids=[ai_run.info.run_id],
        )
        return table
    except Exception:
        logger.info(f"AI Annotations Run for {run_name} not found")

    return None


def get_human_eval_result_or_none(run_name: str) -> Optional[pd.DataFrame]:
    human_run_name = get_human_annotation_run_name(run_name)
    try:
        human_run = get_run_or_fail(human_run_name)
        logger.info(f"Human eval run id: {human_run.info.run_id}")
        return get_human_evals(human_run.info.run_id)
    except Exception:
        logger.warning(f"Human Annotations Run for {run_name} not found")
    return None


def get_latest_human_eval_result_or_none() -> Optional[pd.DataFrame]:
    try:
        human_evals = mlflow.search_runs(
            experiment_names=[EXPERIMENT_NAME],
            filter_string="attributes.run_name LIKE 'human_annotations%'",
            output_format="list",
            order_by=["start_time DESC"],
        )
        logger.info(f"Human evals total found: {len(human_evals)}")
        if len(human_evals) == 0:
            logger.warning("Human Annotations Runs not found")
            return None
        table = mlflow.load_table(
            artifact_file="eval_results_table.json",
            run_ids=[str(human_eval.info.run_id) for human_eval in human_evals],
            extra_columns=["run_id", "start_time", "end_time"],
        )
        filtered_table = keep_human_evals_only(table)
        filtered_table.sort_values(by="start_time", ascending=False, inplace=True)
        return filtered_table.drop_duplicates(subset=["urn", "deployment"])
    except Exception as e:
        logger.exception(f"Error getting latest human eval results: {e}")

    return None
