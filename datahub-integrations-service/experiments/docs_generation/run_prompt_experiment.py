from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import json
import os
import pathlib
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import asyncer
import mlflow
import mlflow.bedrock
import mlflow.metrics
import pandas as pd
import typer
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.experimentation.docs_generation.eval_common import (
    docs_generation_experiments_dir,
    to_entity_info_model,
)
from datahub_integrations.experimentation.docs_generation.metrics import (
    has_table_description_metric_fn,
    has_valid_links_metric_fn,
)
from datahub_integrations.experimentation.docs_generation.mlflow_common import (
    EXPERIMENT_NAME,
)
from datahub_integrations.experimentation.utils import execute_notebook_save_as_html
from datahub_integrations.gen_ai import description_context, description_v3
from datahub_integrations.gen_ai.description_context import (
    transform_table_info_for_llm,
)
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.gen_ai.description_v3 import (
    ANYIO_THREAD_COUNT,
    MAX_COLUMNS_PER_BATCH,
    EntityDescriptionResult,
    generate_entity_descriptions_for_urn_eval_v3,
)

assert AI_EXPERIMENTATION_INITIALIZED
# Global constants
BATCH_SIZE = 5  # Number of files to process concurrently


# This holds main logic for prompt engineering and generation of entity descriptions
@mlflow.trace(name="generate_entity_descriptions_for_urn", span_type="function")
def generate_entity_descriptions_for_urn_eval_wrapper(
    data: Dict[str, Any],
) -> EntityDescriptionResult:
    extracted_entity_info = to_entity_info_model(data["extracted_entity_info"])
    mlflow.update_current_trace(
        tags={"urn": data["urn"], "deployment": data["deployment"]}
    )

    return generate_entity_descriptions_for_urn_eval_v3(
        urn=data["urn"],
        extracted_entity_info=extracted_entity_info,
    )


def process_single_file(
    file: pathlib.Path,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    with open(file, "r") as f:
        data = json.load(f)
        with PerfTimer() as timer:
            try:
                result = generate_entity_descriptions_for_urn_eval_wrapper(data)
            except Exception as e:
                logger.warning(f"Error processing file {file}: {e}")
                result = EntityDescriptionResult(
                    table_description=None,
                    column_descriptions=None,
                    extracted_entity_info=to_entity_info_model(
                        data["extracted_entity_info"]
                    ),
                    failure_reason=str(e),
                )
            generation_time = timer.elapsed_seconds()

        _, column_infos = transform_table_info_for_llm(result.extracted_entity_info)

        # NOTE: Keep only matching columns from the input column_infos
        # Any additional columns from AI generated column descriptions are not included
        column_descs = [
            {
                "urn": data["urn"],
                "deployment": data["deployment"],
                "column": column,
                "description": (
                    result.column_descriptions.get(column)
                    if result.column_descriptions
                    else None
                ),
                "failure_reason": result.failure_reason,
            }
            for column in column_infos
        ]

        table_desc = {
            "urn": data["urn"],
            "deployment": data["deployment"],
            "description": result.table_description,
            "generation_time": generation_time,
            "entity_info": result.extracted_entity_info.model_dump_json(
                exclude_none=True
            ),
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
            "notes": data["notes"],
            "total_columns": len(result.extracted_entity_info.column_names),
            "num_columns_described": len(
                [
                    col
                    for col in column_descs
                    if col["description"] is not None and col["description"] != ""
                ]
            ),
            "percent_columns_described": (
                100
                * len([col for col in column_descs if col["description"] is not None])
                / len(result.extracted_entity_info.column_names)
            ),
        }

        return table_desc, column_descs


def setup_artifact_directory(tempdir: str) -> pathlib.Path:
    artifact_temp_path = pathlib.Path(tempdir) / "artifacts"
    os.makedirs(artifact_temp_path)
    logger.info(f"Artifact temp path: {artifact_temp_path}")
    return artifact_temp_path


def log_artifacts(
    artifact_temp_path: pathlib.Path,
    table_descriptions: List[Dict[str, Any]],
    column_descriptions: List[Dict[str, Any]],
) -> None:
    table_description_artifact_path = artifact_temp_path / "table_descriptions.json"
    with open(table_description_artifact_path, "w") as f:
        json.dump(table_descriptions, f)
        # This log as table does not work due to some forbidden characters in the data
        # mlflow.log_table(table_descriptions, f)
    mlflow.log_artifact(str(table_description_artifact_path))
    column_description_artifact_path = artifact_temp_path / "column_descriptions.json"
    with open(column_description_artifact_path, "w") as f:
        json.dump(column_descriptions, f)
    mlflow.log_artifact(str(column_description_artifact_path))


async def process_files(
    files: List[pathlib.Path],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process files in batches of N tasks at a time.

    Args:
        files: List of file paths to process
    """
    table_descriptions = []
    column_descriptions = []

    # Process files in batches
    for i in range(0, len(files), BATCH_SIZE):
        batch_files = files[i : i + BATCH_SIZE]
        results: List[
            asyncer.SoonValue[Tuple[Dict[str, Any], List[Dict[str, Any]]]]
        ] = []

        async with asyncer.create_task_group() as task_group:
            for file in batch_files:
                result = task_group.soonify(asyncer.asyncify(process_single_file))(file)
                results.append(result)

        # Process results from this batch
        for result in results:
            table_desc, column_descs = result.value
            table_descriptions.append(table_desc)
            column_descriptions.extend(column_descs)

    return table_descriptions, column_descriptions


def log_generation_time_metrics(table_descriptions_df: pd.DataFrame) -> None:
    """Log generation time metrics to MLflow."""
    table_descriptions_df = table_descriptions_df[
        (table_descriptions_df["description"].notna())
        & (table_descriptions_df["description"] != "")
    ]
    generation_times = table_descriptions_df["generation_time"]
    if len(generation_times) > 0:
        mlflow.log_metric("generation_time_max", generation_times.max())
        mlflow.log_metric("generation_time_avg", generation_times.mean())

def log_metadata_extraction_metrics(table_descriptions_df: pd.DataFrame) -> None:
    """Log metadata extraction metrics to MLflow."""
    metadata_extraction_times = table_descriptions_df["metadata_extraction_time"]
    if len(metadata_extraction_times) > 0:
        mlflow.log_metric("metadata_extraction_time_avg", metadata_extraction_times.mean())
        mlflow.log_metric("metadata_extraction_time_max", metadata_extraction_times.max())

def run_experiment(files: List[pathlib.Path], run_description: Optional[str]) -> None:
    logger.info(f"Running experiment with {len(files)} files")
    # if not run_description:
    #    run_description = input("Enter run description: ")
    with (
        mlflow.start_run(description=run_description),
        tempfile.TemporaryDirectory() as tempdir,
    ):
        # log current file as artifact or model
        mlflow.log_artifact(
            str(docs_generation_experiments_dir / "run_prompt_experiment.py")
        )
        mlflow.log_artifact(str(description_v3.__file__))
        mlflow.log_artifact(str(description_context.__file__))
        artifact_temp_path = setup_artifact_directory(tempdir)
        table_descriptions, column_descriptions = asyncer.syncify(
            process_files, raise_sync_error=False
        )(files)
        log_artifacts(artifact_temp_path, table_descriptions, column_descriptions)
        mlflow.log_params(
            {
                "model": model_config.documentation_ai.model,
                "parallel_files": BATCH_SIZE,
                "max_columns_per_batch": MAX_COLUMNS_PER_BATCH,
                "anyio_thread_count": ANYIO_THREAD_COUNT,
            }
        )

        # This adds eval_results_table.json artifact to the run
        # Do not remove this, it is used for ai evaluation and human annotations
        table_descriptions_df = pd.DataFrame(table_descriptions)
        mlflow.evaluate(
            data=table_descriptions_df,
            predictions="description",
            evaluators="default",
            targets="entity_info",
            extra_metrics=[
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
                # *ai_metrics
            ],
        )

        # Calculate and log column metrics separately
        if column_descriptions:
            # Add has_column_description flag to each column entry
            for col_desc in column_descriptions:
                col_desc["has_column_description"] = bool(col_desc.get("description"))

            # Create DataFrame with the enhanced data
            column_df = pd.DataFrame(column_descriptions)

            # Log detailed column description results as a separate table
            mlflow.log_table(column_df, "col_desc_eval_results_table.json")

            # Calculate column metrics
            column_metrics = calculate_column_metrics(column_df)

            # Log the metrics
            for metric_name, value in column_metrics.items():
                mlflow.log_metric(metric_name, value)

        # Log generation time metrics
        log_generation_time_metrics(table_descriptions_df)
        active_run = mlflow.active_run()
        assert active_run is not None
        try:
            html_path = execute_notebook_save_as_html(
                docs_generation_experiments_dir / "analyze_experiment_run.ipynb",
                pathlib.Path(tempdir),
                {"RUN_NAME": active_run.info.run_name},
            )
            mlflow.log_artifact(html_path)
        except Exception as e:
            logger.error(f"Error executing notebook: {e}")


def calculate_column_metrics(column_df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate metrics about column descriptions.

    Args:
        column_df: DataFrame containing column description data

    Returns:
        Dictionary of metrics
    """
    # Calculate metrics from the DataFrame
    total_columns = len(column_df)

    # Group by URN to calculate tables with all columns described
    table_columns = column_df.groupby("urn").agg(
        {"has_column_description": ["count", "sum"]}
    )
    table_columns.columns = ["total", "with_description"]
    tables_with_all_columns_described = sum(
        table_columns["total"] == table_columns["with_description"]
    )
    tables_with_95_percent_columns_described = sum(
        table_columns["with_description"] >= table_columns["total"] * 0.95
    )
    total_tables = len(table_columns)

    # Create metrics dictionary
    return {
        "has_column_description/total_tables": total_tables,
        "has_column_description/total_columns": total_columns,
        "has_column_description/pass_percentage": (
            100 * tables_with_all_columns_described / total_tables
            if total_tables > 0
            else 0.0
        ),
        "has_column_description_95_percent/pass_percentage": (
            100 * tables_with_95_percent_columns_described / total_tables
            if total_tables > 0
            else 0.0
        ),
    }


def run_prompt_experiment(
    run_description: Optional[str] = None, urns: Optional[List[str]] = None
) -> None:
    logger.info(f"eval data directory: {docs_generation_experiments_dir / 'eval_data'}")

    mlflow.set_experiment(EXPERIMENT_NAME)
    eval_data_path = docs_generation_experiments_dir / "eval_data"
    eval_files = []

    for file in list(eval_data_path.glob("*.json")):
        with open(file, "r") as f:
            data = json.load(f)
            if urns and data["urn"] not in urns:
                continue
            eval_files.append(file)
            logger.debug(f"Added file: {file} for urn {data['urn']}")
    # NOTE: modify generate_entity_descriptions_for_urn_eval_wrapper to change prompt
    # or any other inputs for prompt engineering experiments

    run_experiment(eval_files, run_description)


if __name__ == "__main__":
    typer.run(run_prompt_experiment)
