from datahub_integrations.gen_ai.description_v2 import (
    ExtractedTableInfo,
    transform_table_info_for_llm,
)
import streamlit as st
import pandas as pd
import numpy as np
import functools

import argparse

# Create an argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "--run-name", type=str, required=True, help="MLflow run name to process"
)

# Parse the command-line arguments
args = parser.parse_args()


# Access the run_id
run_name = args.run_name

import os
import json
import mlflow
import mlflow.entities
import mlflow.metrics
from typing import Dict, Any, Optional
from pydantic import BaseModel
import dotenv

dotenv.load_dotenv()


# Run mlflow server locally using following command
# python3 -m mlflow server --host 127.0.0.1 --port 9090
EXPERIMENT_NAME = "docs_generation"
mlflow.set_experiment(EXPERIMENT_NAME)


# TODO: move to common
def get_run_or_fail(run_name):
    return mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}'",
        output_format="list",
        order_by=["start_time DESC"],
    )[0]


def get_original_expt_run(run_name):
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )

    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{original_run_name}'",
        output_format="list",
    )
    if len(runs) == 0:
        raise ValueError(
            f"Original Experiment Run {original_run_name} not found in experiment {EXPERIMENT_NAME}"
        )
    if len(runs) > 1:
        print(
            f"Multiple original experiment runs found for {original_run_name} in experiment {EXPERIMENT_NAME}, will pick latest one"
        )

    return runs[0]


# TODO: move to common
def get_human_annotation_run_name(run_name):
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"human_annotations_{original_run_name}"


# TODO: move to common
def get_ai_annotation_run_name(run_name):
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"ai_annotations_{original_run_name}"


input_run = get_run_or_fail(run_name)
run_id = input_run.info.run_id


def get_prefill_human_annotations_run(
    input_run: mlflow.entities.Run,
) -> mlflow.entities.Run:
    original_expt_run = get_original_expt_run(input_run.info.run_name)

    try:
        prefill_run = get_run_or_fail(
            get_human_annotation_run_name(input_run.info.run_name)
        )
        print(
            f"Prefill Human annotations run found {prefill_run.info.run_name} | {prefill_run.info.run_id}"
        )
    except Exception as e:
        print(
            f"Human Annotations Run for {original_expt_run.info.run_name} | {original_expt_run.info.run_id} not found, using run {input_run.info.run_name}| {input_run.info.run_id} instead"
        )
        prefill_run = input_run
    return prefill_run


def get_prefill_ai_annotations_run(
    input_run: mlflow.entities.Run,
) -> mlflow.entities.Run:
    original_expt_run = get_original_expt_run(input_run.info.run_name)

    try:
        prefill_ai_run = get_run_or_fail(
            get_ai_annotation_run_name(input_run.info.run_name)
        )
        print(
            f"Prefill AI annotations run found {prefill_ai_run.info.run_name} | {prefill_ai_run.info.run_id}"
        )
    except Exception as e:
        print(
            f"AI Annotations Run for {original_expt_run.info.run_name} | {original_expt_run.info.run_id} not found, using run {input_run.info.run_name}| {input_run.info.run_id} instead"
        )
        prefill_ai_run = input_run
    return prefill_ai_run


class MetricValue(BaseModel):
    reasoning: Optional[str] = None
    value: Optional[str] = None

    def bool_value(self) -> Optional[bool]:
        if self.value is not None:
            return self.value.strip().lower() == "pass"
        else:
            return None


class IntegerMetricValue(BaseModel):
    value: Optional[int] = 0
    reasoning: Optional[str] = None


class HumanJudgeVerdict(BaseModel):
    class Config:
        extra = "allow"  # Allow extra fields when using parse_obj

    has_source_details: Optional[MetricValue] = None
    has_downstream_usecases: Optional[MetricValue] = None
    has_compliance_insights: Optional[MetricValue] = None
    has_usage_tips: Optional[MetricValue] = None
    is_confident: Optional[MetricValue] = None

    @property
    def overall_score(self) -> Optional[IntegerMetricValue]:
        # Overall score is the sum of all the metrics that are passed (True = 1, False = 0)
        # In future, we can add weights to each metric and calculate the overall score
        score = 0
        metrics = [
            self.has_source_details,
            self.has_downstream_usecases,
            self.has_compliance_insights,
            self.has_usage_tips,
            self.is_confident,
        ]
        for metric in metrics:
            if metric is not None and metric.value is not None:
                score += 1 if metric.bool_value() else 0

        if all(metric is None or metric.value is None for metric in metrics):
            return IntegerMetricValue(value=score, reasoning=None)
        else:
            return IntegerMetricValue(
                value=score, reasoning=f"Overall score is {score} out of {len(metrics)}"
            )


def extract_table_annotation(current_table_name, current_table_index):
    verdict = HumanJudgeVerdict(
        has_source_details=MetricValue(
            value=st.session_state[f"has_source_details_value_{current_table_index}"],
            reasoning=st.session_state[
                f"has_source_details_reasoning_{current_table_index}"
            ],
        ),
        has_downstream_usecases=MetricValue(
            value=st.session_state[
                f"has_downstream_usecases_value_{current_table_index}"
            ],
            reasoning=st.session_state[
                f"has_downstream_usecases_reasoning_{current_table_index}"
            ],
        ),
        has_compliance_insights=MetricValue(
            value=st.session_state[
                f"has_compliance_insights_value_{current_table_index}"
            ],
            reasoning=st.session_state[
                f"has_compliance_insights_reasoning_{current_table_index}"
            ],
        ),
        has_usage_tips=MetricValue(
            value=st.session_state[f"has_usage_tips_value_{current_table_index}"],
            reasoning=st.session_state[
                f"has_usage_tips_reasoning_{current_table_index}"
            ],
        ),
        is_confident=MetricValue(
            value=st.session_state[f"is_confident_value_{current_table_index}"],
            reasoning=st.session_state[f"is_confident_reasoning_{current_table_index}"],
        ),
    )
    # Store the annotation with table information
    annotation = {
        "table_name": current_table_name,
        "table_index": current_table_index,
        "verdict": verdict.dict(),
    }

    return annotation


# Create expanders for each metric in the HumanJudgeVerdict model
def get_metric(metrics_dict, metric_name):
    metric_val = metrics_dict.get(f"{metric_name}/score", False)
    value = "pass" if (metric_val is True or metric_val == 1) else "fail"
    reasoning = metrics_dict.get(f"{metric_name}/justification", "")

    return value, reasoning


# Always pick previous human annotations for same description(original run id) as defaults on UI + as well
# as report them as part of human annotations run
# This is necessary due to immutable nature of mlflow runs, we always create new runs for every evaluation
# and we want to avoid duplicating human annotations for same description
prefill_run = get_prefill_human_annotations_run(input_run)
prefill_ai_run = get_prefill_ai_annotations_run(input_run)
experiment_id = prefill_run.info.experiment_id

# Load data from MLflow run
# Get the evaluation results from the run artifacts
artifact_path = "eval_results_table.json"
eval_results = mlflow.load_table(
    artifact_file=artifact_path, run_ids=[prefill_run.info.run_id]
)
ai_eval_results = mlflow.load_table(
    artifact_file=artifact_path, run_ids=[prefill_ai_run.info.run_id]
).set_index("urn")
# Initialize session state
if "current_table_index" not in st.session_state:
    st.session_state.current_table_index = 0
if "annotations" not in st.session_state:
    st.session_state.annotations = {}

# Prepare table data for UI
table_data = {}
table_names = []
table_display_names = []
for idx, row in eval_results.iterrows():
    table_name = row["urn"]

    # Extract description
    description = (
        row["description"]
        if pd.notna(row["description"])
        else "No description available"
    )

    # Extract evaluation metrics
    metrics = {
        col: row[col]
        for col in eval_results.columns
        if col.startswith("has_") or col.startswith("is_") or col.startswith("overall_")
    }

    ai_metrics = {
        col: ai_eval_results.loc[table_name][col]
        for col in ai_eval_results.columns
        if table_name in ai_eval_results.index
        and (
            col.startswith("has_")
            or col.startswith("is_")
            or col.startswith("overall_")
        )
    }
    table_data[table_name] = {
        "urn": table_name,
        "deployment": row["deployment"],
        "description": description,
        "data": row["entity_info"],
        "metrics": metrics,
        "ai_metrics": ai_metrics,
    }
    table_names.append(table_name)
    # Include deployment name in table display name
    table_display_names.append(f"{row['deployment']} - {table_name}")

    metrics_names = [
        "has_source_details",
        "has_downstream_usecases",
        "has_compliance_insights",
        "has_usage_tips",
        "is_confident",
    ]

    if prefill_run.info.run_name.startswith("human_annotations_") and any(
        table_data[table_name]["metrics"].get(f"{metric}/justification")
        for metric in metrics_names
    ):
        for metric in metrics_names:
            default_value, default_reasoning = get_metric(
                table_data[table_name]["metrics"], metric
            )
            if f"{metric}_value_{idx}" not in st.session_state:
                st.session_state[f"{metric}_value_{idx}"] = default_value
            if f"{metric}_reasoning_{idx}" not in st.session_state:
                st.session_state[f"{metric}_reasoning_{idx}"] = default_reasoning

        if table_name not in st.session_state.annotations:
            annotation = extract_table_annotation(table_name, idx)
            st.session_state.annotations[table_name] = annotation
    else:
        for metric in metrics_names:
            default_value, default_reasoning = get_metric(
                table_data[table_name]["ai_metrics"], metric
            )

            if f"{metric}_value_{idx}" not in st.session_state:
                st.session_state[f"{metric}_value_{idx}"] = default_value
            if f"{metric}_reasoning_{idx}" not in st.session_state:
                st.session_state[f"{metric}_reasoning_{idx}"] = default_reasoning


# Custom CSS to increase max-width
st.markdown(
    """
    <style>
   .stMainBlockContainer {
        max-width: 1536px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Table Description Evaluation Tool")
st.markdown(
    f"MLflow Experiment ID: {experiment_id} | Run ID: {run_id} | [View in MLflow]({mlflow.get_tracking_uri()}/#/experiments/{experiment_id}/runs/{run_id})"
)


# Table selector
def update_table_index():
    st.session_state.current_table_index = table_display_names.index(
        st.session_state.table_selector
    )


selected_table = st.selectbox(
    "Select a table to review",
    options=table_display_names,
    index=st.session_state.current_table_index,
    key="table_selector",
    on_change=update_table_index,
)

current_table = table_names[st.session_state.current_table_index]
# Progress Bar
progress_percent = (st.session_state.current_table_index + 1) / len(table_names)
st.progress(progress_percent)

st.write(
    f"Table {st.session_state.current_table_index + 1} of {len(table_names)} (Annotated {len(st.session_state.annotations)} tables so far)"
)
st.markdown(
    f"Table Name: [{current_table}](https://{table_data[current_table]['deployment']}.acryl.io/login?redirect_uri=/dataset/{current_table})"
)

if st.session_state.annotations.get(current_table):
    st.markdown(":green[This table has already been annotated.]")

with st.expander("**Table Info and Column Infos**", expanded=False):
    info = ExtractedTableInfo.parse_obj(table_data[current_table]["data"])
    table_info, column_info = transform_table_info_for_llm(info)
    st.json(table_info.dict(exclude_none=True))
    st.json({k: col.dict(exclude_none=True) for k, col in column_info.items()})


with st.expander("**Generated Description**", expanded=True):
    tab1, tab2 = st.tabs(["Raw", "Formatted"])
    with tab1:
        st.text(table_data[current_table]["description"])
    with tab2:
        st.markdown(table_data[current_table]["description"])


st.subheader(
    "Add Human Annotations (Pre-filled with AI Generated Metrics, if available):"
)


r1col1, r1col2, r1col3 = st.columns(3)
r2col1, r2col2, r2col3 = st.columns(3)


def get_index_from_value(value):
    return ["pass", "fail"].index(value)


with r1col1:
    with st.expander("Source Details", expanded=True):
        has_source_details_value = st.radio(
            "Does the description provide correct details about the source of the data?",
            ["pass", "fail"],
            key=f"has_source_details_value_{st.session_state.current_table_index}",
            index=get_index_from_value(
                st.session_state.get(
                    f"has_source_details_value_{st.session_state.current_table_index}"
                )
            ),
        )
        has_source_details_reasoning = st.text_area(
            "Reasoning:",
            key=f"has_source_details_reasoning_{st.session_state.current_table_index}",
            value=st.session_state.get(
                f"has_source_details_reasoning_{st.session_state.current_table_index}"
            ),
        )

with r1col2:
    with st.expander("Downstream Use Cases", expanded=True):
        has_downstream_usecases_value = st.radio(
            "Does the description mention correct downstream use cases?",
            ["pass", "fail"],
            key=f"has_downstream_usecases_value_{st.session_state.current_table_index}",
            index=get_index_from_value(
                st.session_state.get(
                    f"has_downstream_usecases_value_{st.session_state.current_table_index}"
                )
            ),
        )
        has_downstream_usecases_reasoning = st.text_area(
            "Reasoning:",
            key=f"has_downstream_usecases_reasoning_{st.session_state.current_table_index}",
            value=st.session_state.get(
                f"has_downstream_usecases_reasoning_{st.session_state.current_table_index}"
            ),
        )

with r1col3:
    with st.expander("Compliance Insights", expanded=True):
        has_compliance_insights_value = st.radio(
            "Does the description provide correct compliance insights?",
            ["pass", "fail"],
            key=f"has_compliance_insights_value_{st.session_state.current_table_index}",
            index=get_index_from_value(
                st.session_state.get(
                    f"has_compliance_insights_value_{st.session_state.current_table_index}"
                )
            ),
        )
        has_compliance_insights_reasoning = st.text_area(
            "Reasoning:",
            key=f"has_compliance_insights_reasoning_{st.session_state.current_table_index}",
            value=st.session_state.get(
                f"has_compliance_insights_reasoning_{st.session_state.current_table_index}"
            ),
        )

with r2col1:
    with st.expander("Content and Usage Tips", expanded=True):
        has_usage_tips_value = st.radio(
            "Does the description provide correct content and usage tips?",
            ["pass", "fail"],
            key=f"has_usage_tips_value_{st.session_state.current_table_index}",
            index=get_index_from_value(
                st.session_state.get(
                    f"has_usage_tips_value_{st.session_state.current_table_index}"
                )
            ),
        )
        has_usage_tips_reasoning = st.text_area(
            "Reasoning:",
            key=f"has_usage_tips_reasoning_{st.session_state.current_table_index}",
            value=st.session_state.get(
                f"has_usage_tips_reasoning_{st.session_state.current_table_index}"
            ),
        )

with r2col2:
    with st.expander("Confidence", expanded=True):
        is_confident_value = st.radio(
            "Does the description convey confidence?",
            ["pass", "fail"],
            key=f"is_confident_value_{st.session_state.current_table_index}",
            index=get_index_from_value(
                st.session_state.get(
                    f"is_confident_value_{st.session_state.current_table_index}"
                )
            ),
        )
        is_confident_reasoning = st.text_area(
            "Reasoning:",
            key=f"is_confident_reasoning_{st.session_state.current_table_index}",
            value=st.session_state.get(
                f"is_confident_reasoning_{st.session_state.current_table_index}"
            ),
        )


def log_mlflow_run_with_human_annotations(
    prefill_run: mlflow.entities.Run, annotations
):
    def common_eval_fn(table_description, urn) -> HumanJudgeVerdict:
        # TODO: why quote in urn?
        urn = urn.strip('"')

        annotation = annotations.get(urn, None)
        if annotation is None:
            verdict = HumanJudgeVerdict()
        else:
            verdict = HumanJudgeVerdict.parse_obj(annotation["verdict"])
        return verdict

    def custom_eval_fn_metric(metric, predictions, targets):
        scores = []
        justifications = []
        for prediction, target in zip(predictions, targets):
            judged_metric = getattr(
                common_eval_fn(prediction, json.dumps(target)), metric
            )
            if judged_metric is not None:
                scores.append(judged_metric.bool_value())
                justifications.append(judged_metric.reasoning)
            else:
                scores.append(None)
                justifications.append(None)

        return mlflow.metrics.MetricValue(
            scores=scores,
            justifications=justifications,
            aggregate_results={
                "pass_percentage": len(list(filter(lambda x: x, scores))) / len(scores)
            },
        )

    def overall_score_eval_fn(predictions, targets):
        scores = []
        justifications = []
        for prediction, target in zip(predictions, targets):
            judged_metric = common_eval_fn(prediction, json.dumps(target)).overall_score
            if judged_metric is not None:
                scores.append(judged_metric.value)
                justifications.append(judged_metric.reasoning)
            else:
                scores.append(0)
                justifications.append(None)

        return mlflow.metrics.MetricValue(
            scores=scores,
            justifications=justifications,
            aggregate_results={
                "mean": np.mean(scores),
                "variance": np.var(scores),
                "median": np.median(scores),
            },
        )

    def make_overall_score_metric():
        return mlflow.metrics.make_metric(
            eval_fn=overall_score_eval_fn, greater_is_better=True, name="overall_score"
        )

    def make_custom_metric(metric_name):
        """Mlflow custom AI metric allows generating single metric from single prompt.
        Since we need to generate multiple metrics from single prompt, we are using mlflow.metrics.make_metric
        with cache powered custom eval_fn that can generate multiple metrics from single prompt, instead of using
        mlflow.metrics.make_gen_ai_metric that can only generate single metric from single prompt.
        """
        return mlflow.metrics.make_metric(
            eval_fn=functools.partial(custom_eval_fn_metric, metric_name),
            greater_is_better=True,
            name=metric_name,
        )

    metric_has_source_details = make_custom_metric("has_source_details")
    metric_has_downstream_usecases = make_custom_metric("has_downstream_usecases")
    metric_has_compliance_insights = make_custom_metric("has_compliance_insights")
    metric_has_usage_tips = make_custom_metric("has_usage_tips")
    metric_is_confident = make_custom_metric("is_confident")
    metric_overall_score = make_overall_score_metric()

    new_run_name = get_human_annotation_run_name(prefill_run.info.run_name)

    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name=new_run_name,
        tags={
            "evaluation_type": "human",
        },
    ) as new_run:
        mlflow.evaluate(
            data=eval_results,
            predictions="description",
            evaluators="default",
            targets="urn",
            extra_metrics=[
                metric_has_source_details,
                metric_has_downstream_usecases,
                metric_has_compliance_insights,
                metric_has_usage_tips,
                metric_is_confident,
                metric_overall_score,
                # mlflow.metrics.toxicity(),
                # mlflow.metrics.ari_grade_level(),
                # mlflow.metrics.flesch_kincaid_grade_level(),
            ],
        )
        run_url = f"{mlflow.get_tracking_uri()}/#/experiments/{new_run.info.experiment_id}/runs/{new_run.info.run_id}"
        st.session_state.textarea_content = (
            f"Successfully logged all annotations ! [Annotations Run URL]({run_url})"
        )


# Callback function
def add_table_annotations():
    # TODO: scroll to top
    current_table_name = table_names[st.session_state.current_table_index]
    current_table_index = st.session_state.current_table_index

    # Create a HumanJudgeVerdict instance with the current annotations
    annotation = extract_table_annotation(current_table_name, current_table_index)

    st.session_state.annotations[current_table_name] = annotation
    if st.session_state.current_table_index < len(table_names) - 1:
        st.session_state.current_table_index += 1


def log_results():
    # log mlflow run with annotations
    log_mlflow_run_with_human_annotations(prefill_run, st.session_state.annotations)


def done():
    # add_table_annotations()
    log_results()
    st.write("All annotations logged!")


# Button with callback
st.button("**Submit and Go to Next**", on_click=add_table_annotations)

st.write(f"Annotated {len(st.session_state.annotations)} tables so far")
with st.expander("Annotations Preview", expanded=False):
    st.json(st.session_state.annotations)

st.button("**Submit and Finish**", on_click=done)
# add editable comment section
if st.session_state.get("textarea_content"):
    st.markdown(st.session_state.textarea_content)
