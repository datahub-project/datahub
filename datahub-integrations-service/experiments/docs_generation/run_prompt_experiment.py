import json
import pathlib
import os
import tempfile
from typing import List, Dict, Tuple
import asyncer

current_dir = pathlib.Path().resolve()


# Create a .env file in eval directory
# Set env variables BEDROCK_AWS_ACCESS_KEY_ID, BEDROCK_AWS_SECRET_ACCESS_KEY,BEDROCK_AWS_REGION
import dotenv

dotenv.load_dotenv()

from datahub.utilities.perf_timer import PerfTimer

from datahub_integrations.gen_ai.description_v2 import (
    ExtractedTableInfo,
    PROMPT_TEMPLATE,
    EntityDescriptionResult,
    parse_llm_output,
    transform_table_info_for_llm,
    call_bedrock_llm,
    DESCRIPTION_GENERATION_MODEL,
)
from datahub_integrations.gen_ai.router import DescriptionV2ParsingError
import pandas as pd

import mlflow
import mlflow.metrics
import mlflow.bedrock


print("eval directory", current_dir)
print("parent directory", current_dir.parent)
print("eval data directory", current_dir / "eval_data")

CURRENT_PROMPT = PROMPT_TEMPLATE


# This holds main logic for prompt engineering and generation of entity descriptions
@mlflow.trace(name="generate_entity_descriptions_for_urn", span_type="function")
def generate_entity_descriptions_for_urn_eval_wrapper(data):
    extracted_entity_info = ExtractedTableInfo.parse_obj(data["extracted_entity_info"])
    mlflow.update_current_trace(
        tags={"urn": data["urn"], "deployment": data["deployment"]}
    )
    # mlflow_prompt = mlflow.load_prompt("prompts:/docs-generation-prompt/1").template

    table_info, column_infos = transform_table_info_for_llm(extracted_entity_info)
    formatted_prompt = CURRENT_PROMPT.format(
        table_info=table_info.dict(exclude_none=True),
        column_info={
            col: column_info.dict(exclude_none=True)
            for col, column_info in column_infos.items()
        },
    )
    llm_output = call_bedrock_llm(
        prompt=formatted_prompt, model=DESCRIPTION_GENERATION_MODEL, max_tokens=5000
    )
    table_description, column_descriptions, failure_reason = parse_llm_output(
        llm_output
    )

    return EntityDescriptionResult(
        table_description=table_description,
        column_descriptions=column_descriptions,
        extracted_entity_info=extracted_entity_info,
        raw_llm_output=llm_output,
        failure_reason=failure_reason,
    )


def process_single_file(file) -> Tuple[Dict, List[Dict]]:
    with open(file, "r") as f:
        data = json.load(f)
        with PerfTimer() as timer:
            try:
                result = generate_entity_descriptions_for_urn_eval_wrapper(data)
            except Exception as e:
                print(f"Error processing file {file}: {e}")
                result = EntityDescriptionResult(
                    table_description=None,
                    column_descriptions=None,
                    extracted_entity_info=ExtractedTableInfo.parse_obj(
                        data["extracted_entity_info"]
                    ),
                    raw_llm_output=None,
                )
            generation_time = timer.elapsed_seconds()

        table_desc = {
            "urn": data["urn"],
            "deployment": data["deployment"],
            "description": result.table_description,
            "generation_time": generation_time,
            "entity_info": result.extracted_entity_info.dict(),
            "has_schema": len(result.extracted_entity_info.column_names) > 0,
            "has_upstreams": (
                len(result.extracted_entity_info.table_upstream_lineage_info) > 0
                if result.extracted_entity_info.table_upstream_lineage_info
                else False
            ),
            "has_downstreams": len(
                result.extracted_entity_info.table_downstream_lineage_info
            )
            > 0,
            "failure_reason": result.failure_reason,
        }

        column_descs = (
            [
                {
                    "urn": data["urn"],
                    "deployment": data["deployment"],
                    "column": column,
                    "description": description,
                }
                for column, description in result.column_descriptions.items()
            ]
            if result.column_descriptions
            else []
        )

        return table_desc, column_descs


def setup_artifact_directory(tempdir):
    artifact_temp_path = pathlib.Path(tempdir) / "artifacts"
    os.makedirs(artifact_temp_path)
    print("Artifact temp path", artifact_temp_path)
    return artifact_temp_path


def log_artifacts(artifact_temp_path, table_descriptions, column_descriptions):
    table_description_artifact_path = artifact_temp_path / "table_descriptions.json"
    with open(table_description_artifact_path, "w") as f:
        json.dump(table_descriptions, f)
        # This log as table does not work due to some forbidden characters in the data
        # mlflow.log_table(table_descriptions, f)
    mlflow.log_artifact(table_description_artifact_path)
    column_description_artifact_path = artifact_temp_path / "column_descriptions.json"
    with open(column_description_artifact_path, "w") as f:
        json.dump(column_descriptions, f)
    mlflow.log_artifact(column_description_artifact_path)

    # log current file as artifact or model
    mlflow.log_artifact("./run_prompt_experiment.py")


async def process_files(files):
    table_descriptions = []
    column_descriptions = []
    results: List[asyncer.SoonValue[Tuple[Dict, List[Dict]]]] = []
    async with asyncer.create_task_group() as task_group:
        for file in files:
            result = task_group.soonify(asyncer.asyncify(process_single_file))(file)
            results.append(result)

    for result in results:
        table_desc, column_descs = result.value
        table_descriptions.append(table_desc)
        column_descriptions.extend(column_descs)
    return table_descriptions, column_descriptions


def has_description_metric_fn(predictions, targets):
    scores = [
        (desc is not None and desc != "") for desc, target in zip(predictions, targets)
    ]
    return mlflow.metrics.MetricValue(
        scores=scores,
        aggregate_results={
            "pass_percentage": len(list(filter(lambda x: x, scores))) / len(scores)
        },
    )


def run_experiment(files):
    with mlflow.start_run() as run, tempfile.TemporaryDirectory() as tempdir:
        mlflow.autolog()
        artifact_temp_path = setup_artifact_directory(tempdir)
        table_descriptions, column_descriptions = asyncer.syncify(
            process_files, raise_sync_error=False
        )(files)
        log_artifacts(artifact_temp_path, table_descriptions, column_descriptions)

        has_description_metric = mlflow.metrics.make_metric(
            eval_fn=has_description_metric_fn,
            name="has_description",
            greater_is_better=True,
        )

        # This adds eval_results_table.json artifact to the run
        # Do not remove this, it is used for ai evaluation and human annotations
        mlflow.evaluate(
            data=pd.DataFrame(table_descriptions),
            predictions="description",
            evaluators="default",
            targets="entity_info",
            extra_metrics=[
                has_description_metric,
                # *ai_metrics
            ],
        )


if __name__ == "__main__":
    # Warning: This experiment may take a long time (~15 minutes - 10 for description generation and 5 for ai evaluation, if enabled) to run as it processes multiple files and logs results to MLflow
    # The experiment will be logged under the 'docs_generation' experiment in MLflow
    EXPERIMENT_NAME = "docs_generation"
    mlflow.set_experiment(EXPERIMENT_NAME)

    eval_data_path = current_dir / "eval_data"
    eval_files = list(eval_data_path.glob("*.json"))

    # NOTE: modify generate_entity_descriptions_for_urn_eval_wrapper to change prompt
    # or any other inputs for prompt engineering experiments

    run_experiment(eval_files)
