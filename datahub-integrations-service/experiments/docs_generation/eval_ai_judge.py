import pandas as pd
import dotenv

dotenv.load_dotenv()
import mlflow
import datetime
import typer

EXPERIMENT_NAME = "docs_generation"
mlflow.set_experiment(EXPERIMENT_NAME)
from datahub.ingestion.api.report_helpers import format_datetime_relative

from run_ai_annotations import ai_metrics, get_ai_annotation_run_name

# TODO: move to common
METRIC_NAMES = [
    "has_source_details",
    "has_downstream_usecases",
    "has_compliance_insights",
    "has_usage_tips",
    "is_confident",
]


# TODO: move to common
def get_run_or_fail(run_name):
    return mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}'",
        output_format="list",
        order_by=["start_time DESC"],
    )[0]


def format_time(time: float) -> str:
    return format_datetime_relative(datetime.datetime.fromtimestamp(time / 1000))


# TODO: move to common
def get_human_evals(run_id: str) -> pd.DataFrame:
    artifact_path = "eval_results_table.json"
    table = mlflow.load_table(
        artifact_file=artifact_path,
        run_ids=[run_id],
    )
    return table[
        table["has_source_details/justification"].notna()
        | table["has_downstream_usecases/justification"].notna()
        | table["has_compliance_insights/justification"].notna()
        | table["has_usage_tips/justification"].notna()
        | table["is_confident/justification"].notna()
    ]


# TODO: move to common
def get_human_annotation_run_name(run_name):
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"human_annotations_{original_run_name}"


def eval_ai_judge_accuracy(ai_annotation_run_id: str, run_name: str) -> pd.DataFrame:
    ai_annotation_run = mlflow.get_run(ai_annotation_run_id)
    human_annotation_run = get_run_or_fail(
        get_human_annotation_run_name(run_name)
    )

    print(
        f"Comparing human annotations run {human_annotation_run.info.run_id} created at {format_time(human_annotation_run.info.start_time)}"
        f" with AI annotations run {ai_annotation_run.info.run_id} created at {format_time(ai_annotation_run.info.start_time)}"
    )
    artifact_path = "eval_results_table.json"
    ai_eval_results = mlflow.load_table(
        artifact_file=artifact_path, run_ids=[ai_annotation_run.info.run_id]
    ).set_index("urn")
    human_eval_results = get_human_evals(human_annotation_run.info.run_id).set_index(
        "urn"
    )

    mismatch_urns = set()
    metric_evals = {
        metric_name: {
            "matches": [],
            "mismatches": [],
            "mismatch_reason": [],
            "ai_justification": [],
            "missing": [],
        }
        for metric_name in METRIC_NAMES
    }
    for metric_name in METRIC_NAMES:
        score_col = f"{metric_name}/score"
        reason_col = f"{metric_name}/justification"
        for idx, row in human_eval_results.iterrows():
            if row[score_col] is not None:
                if idx in ai_eval_results.index:
                    if row[score_col] == ai_eval_results.loc[idx][score_col]:
                        metric_evals[metric_name]["matches"].append(idx)
                    else:
                        metric_evals[metric_name]["mismatches"].append(idx)
                        metric_evals[metric_name]["mismatch_reason"].append(
                            row[reason_col]
                        )
                        metric_evals[metric_name]["ai_justification"].append(
                            ai_eval_results.loc[idx][reason_col]
                        )
                        mismatch_urns.add(idx)
                else:
                    metric_evals[metric_name]["missing"].append(idx)
    metric_eval_list = []
    for metric_name in METRIC_NAMES:
        len_matches = len(metric_evals[metric_name]["matches"])
        len_total = len_matches + len(metric_evals[metric_name]["mismatches"])

        metric_eval_list.append(
            {
                "metric": metric_name,
                "matches": len_matches,
                "total": len_total,
                "match_percentage": (100 * len_matches / len_total),
                "mismatch_reasons": metric_evals[metric_name]["mismatch_reason"],
                "ai_justification": metric_evals[metric_name]["ai_justification"],
            }
        )

    eval_df = pd.DataFrame(metric_eval_list)
    return eval_df


def run_eval_ai_judge_experiment(run_name: str, existing_run: bool = False):
    human_annotation_run = get_run_or_fail(get_human_annotation_run_name(run_name))
    ha_df = get_human_evals(human_annotation_run.info.run_id)
    artifact_path = "eval_results_table.json"

    if existing_run:
        last_ai_annotation_run = get_run_or_fail(get_ai_annotation_run_name(run_name))
        table_descriptions = mlflow.load_table(
            artifact_file=artifact_path, run_ids=[last_ai_annotation_run.info.run_id]
        )
    else:
        prompt_experiment_run = get_run_or_fail(run_name)

        table_descriptions = mlflow.load_table(
            artifact_file=artifact_path, run_ids=[prompt_experiment_run.info.run_id]
        )

    with mlflow.start_run(
        experiment_id=human_annotation_run.info.experiment_id,
        run_name=get_ai_annotation_run_name(run_name),
    ):
        mlflow.autolog()

        mlflow.set_tag("evaluation_type", "eval_ai_judge")

        if existing_run:
            mlflow.set_tag("existing_run", "true")
            ai_annotation_run_id = last_ai_annotation_run.info.run_id
        else:
            mlflow.set_tag("existing_run", "false")
            # this will force ai annotations to be run on the same data as human annotations
            table_descriptions = table_descriptions[
                table_descriptions["urn"].isin(ha_df["urn"].tolist())
            ]

            mlflow.evaluate(
                data=table_descriptions,
                predictions="description",
                evaluators="default",
                targets="entity_info",
                extra_metrics=[*ai_metrics],
            )

            mlflow.log_artifact("./run_ai_annotations.py")
            ai_annotation_run_id = mlflow.active_run().info.run_id

        #TODO: add more details/plots about which cases (passing/failing, specific scenarios) are not evaluated
        # as expected for ai judge

        mlflow.log_artifact("./eval_ai_judge.py")
        eval_df = eval_ai_judge_accuracy(ai_annotation_run_id, run_name)

        mlflow.set_tag("evaluation_type", "ai_judge_eval")

        mlflow.log_table(eval_df, "ai_judge_accuracy.json")
        eval_df_idx = eval_df.set_index("metric")
        for metric_name in METRIC_NAMES:
            mlflow.log_metric(
                f"{metric_name}_match_percentage",
                eval_df_idx.loc[metric_name]["match_percentage"],
            )


if __name__ == "__main__":
    typer.run(run_eval_ai_judge_experiment)
