from typing import Optional
import dotenv
import os
dotenv.load_dotenv(".env")

import mlflow
import mlflow.metrics

mlflow.get_tracking_uri()
EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME")
mlflow.set_experiment(EXPERIMENT_NAME)

EXPT_RUN_NAME = "initial_run"

experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
assert experiment
experiment_id = experiment.experiment_id


import json
import pandas as pd
from run_prompt_experiment import has_description_metric_fn

has_description_metric = mlflow.metrics.make_metric(
    eval_fn=has_description_metric_fn,
    name="has_description",
    greater_is_better=True,
)


def import_prompt_experiment(
    eval_file_path: str,
    has_human_annotations: bool = True,
    run_name: Optional[str] = EXPT_RUN_NAME,
):
    with open(eval_file_path) as f:
        human_eval = json.load(f)

    human_eval_df = pd.DataFrame(columns=human_eval["columns"], data=human_eval["data"])
    human_eval_df_original_expt = human_eval_df.drop(
        columns=[
            col
            for col in human_eval_df.columns
            if col.endswith("/score") or col.endswith("/justification")
        ]
    )
    original_expt_run_id = None

    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name):
        mlflow.evaluate(
            data=human_eval_df_original_expt,
            predictions="description",
            evaluators="default",
            targets="entity_info",
            extra_metrics=[
                has_description_metric,
            ],
        )
        active_run = mlflow.active_run()
        assert active_run
        original_expt_run_id = active_run.info.run_id

    if has_human_annotations:
        with mlflow.start_run(
            experiment_id=experiment_id,
            run_name=f"human_annotations_{run_name}",
            tags={
                "evaluation_type": "human",
            },
        ):
            mlflow.evaluate(
                data=human_eval_df,
                predictions="description",
                evaluators="default",
                targets="entity_info",
                extra_metrics=[
                    has_description_metric,
                    # *ai_metrics
                ],
            )


if __name__ == "__main__":
    import typer

    typer.run(import_prompt_experiment)
