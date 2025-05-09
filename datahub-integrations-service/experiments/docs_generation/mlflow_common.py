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

dotenv.load_dotenv()
EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME")
mlflow.set_experiment(EXPERIMENT_NAME)


def get_run_or_fail(run_name, additional_filter_string=""):
    return mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}' {additional_filter_string}",
        output_format="list",
        order_by=["start_time DESC"],
    )[0]


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
            'and tags.evaluation_type!="eval_ai_judge" and tags.evaluation_type!="ai_judge_eval"',
        )
        print("AI run id", ai_run.info.run_id)

        artifact_path = "eval_results_table.json"

        table = mlflow.load_table(
            artifact_file=artifact_path,
            run_ids=[ai_run.info.run_id],
        )
        return table
    except Exception:
        print(f"AI Annotations Run for {run_name} not found")

    return None


def get_human_eval_result_or_none(run_name: str) -> Optional[pd.DataFrame]:
    human_run_name = get_human_annotation_run_name(run_name)
    try:
        human_run = get_run_or_fail(human_run_name)
        print("Human eval run id", human_run.info.run_id)
        return get_human_evals(human_run.info.run_id)
    except Exception:
        print(f"Human Annotations Run for {run_name} not found")
    return None
