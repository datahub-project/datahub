import argparse
import functools
import json
import os

import dotenv
import mlflow
import mlflow.entities
import mlflow.metrics
import numpy as np
import pandas as pd
import streamlit as st
from eval_common import (
    METRIC_NAMES,
    METRICS_CONFIG,
    AIJudgeVerdict,
    HumanGuidelines,
    HumanJudgeVerdict,
    JudgedMetricValue,
    get_deployment_details,
    get_human_annotation_run_name,
    get_human_guidelines,
    get_overall_score,
    update_table_guidelines,
)
from mlflow_common import (
    get_ai_eval_result_or_none,
    get_human_eval_result_or_none,
    get_latest_human_eval_result_or_none,
    get_run_or_fail,
    load_eval_table,
)
from run_ai_annotations import llm_judge_common_eval_fn

from datahub_integrations.gen_ai.description_v2 import (
    ExtractedTableInfo,
    transform_table_info_for_llm,
)

dotenv.load_dotenv()
# Create an argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "--run-name", type=str, required=True, help="MLflow run name to process"
)

# Parse the command-line arguments
args = parser.parse_args()
EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME")
mlflow.set_experiment(EXPERIMENT_NAME)

# Access the run_id
input_run_name = args.run_name


# --- Helper Functions ---
@st.cache_data
def get_input_run(input_run_name):
    return get_run_or_fail(input_run_name)


@st.cache_data
def get_eval_table(input_run_id):
    return load_eval_table(input_run_id)


def row_to_ai_judge_verdict(row: pd.Series) -> AIJudgeVerdict:
    return AIJudgeVerdict.parse_obj(
        {
            metric_name: JudgedMetricValue.parse_obj(
                {
                    "value": row[f"{metric_name}/score"],
                    "reasoning": row[f"{metric_name}/justification"],
                }
            )
            for metric_name in METRIC_NAMES
        }
    )


def row_to_human_judge_verdict(
    row: pd.Series, guidelines: HumanGuidelines
) -> HumanJudgeVerdict:
    return HumanJudgeVerdict.parse_obj(
        {
            metric_name: JudgedMetricValue.parse_obj(
                {
                    "value": row[f"{metric_name}/score"],
                    "reasoning": row[f"{metric_name}/justification"],
                    "guidelines": (
                        "\n".join(guidelines.guidelines[metric_name])
                        if guidelines.guidelines.get(metric_name) is not None
                        else row[f"{metric_name}/justification"]
                    ),
                }
            )
            for metric_name in METRIC_NAMES
            # if f"{metric_name}/score" in row
        }
    )


def show_table(current_table):
    current_table_data = st.session_state.table_data[current_table]

    # reset all the values
    for metric_name in METRIC_NAMES:
        st.session_state[f"{metric_name}_value"] = "fail"
        st.session_state[f"{metric_name}_guidelines"] = (
            "\n".join(current_table_data["guidelines"].guidelines[metric_name])
            if current_table_data["guidelines"] is not None
            and current_table_data["guidelines"].guidelines is not None
            and metric_name in current_table_data["guidelines"].guidelines
            else ""
        )  # show guidelines initialized from file unless user has edited it
        # submitted the annotation
        st.session_state[f"{metric_name}_ai_value"] = "pass"
        st.session_state[f"{metric_name}_ai_reasoning"] = ""
        st.session_state[f"{metric_name}_ai_rerun_msg"] = ""

    if st.session_state.annotations.get(current_table):
        verdict = HumanJudgeVerdict.parse_obj(
            st.session_state.annotations[current_table]["verdict"]
        )

        for metric_name in METRIC_NAMES:
            metric_guidelines = []
            # Only if guidelines are not initialized from file, we are using the
            # guidelines from earlier human annotations run to update the guidelines
            # This is more of init hack to ensure that we are not losing the earlier
            # human annotations
            if (
                (
                    current_table_data["guidelines"] is None
                    or current_table_data["guidelines"].guidelines is None
                    or current_table_data["guidelines"].guidelines.get(metric_name)
                    is None
                )
                and hasattr(verdict, metric_name)
                and getattr(verdict, metric_name) is not None
                and getattr(verdict, metric_name).guidelines is not None
            ):
                metric_guidelines = getattr(verdict, metric_name).guidelines
                st.session_state[f"{metric_name}_guidelines"] = "\n".join(
                    metric_guidelines
                )
            st.session_state[f"{metric_name}_value"] = getattr(
                verdict, metric_name
            ).value

    if st.session_state.table_data[current_table]["ai_eval_results"]:
        verdict = st.session_state.table_data[current_table]["ai_eval_results"]
        try:
            for metric_name in METRIC_NAMES:
                if (
                    getattr(verdict, metric_name) is not None
                    and getattr(verdict, metric_name).value is not None
                ):
                    st.session_state[f"{metric_name}_ai_value"] = getattr(
                        verdict, metric_name
                    ).value
                    st.session_state[f"{metric_name}_ai_reasoning"] = getattr(
                        verdict, metric_name
                    ).reasoning
        except Exception as e:
            print("Error setting AI eval results", e, verdict)


def prefill_annotations(human_eval_result, guidelines):
    for _, row in human_eval_result.iterrows():
        st.session_state.annotations[row["urn"]] = {
            "verdict": row_to_human_judge_verdict(
                row, guidelines.get(row["urn"])
            ).dict(),
            "table_name": row["urn"],
        }


# Initialize session state
def init_session_state_with_previous_annotations(run_name, eval_result):
    print("initializing session state with previous annotations")
    st.session_state.annotations = {}

    human_eval_result = get_human_eval_result_or_none(run_name)

    ai_eval_result = get_ai_eval_result_or_none(run_name)
    assert ai_eval_result is not None

    ai_eval_result_indexed = ai_eval_result.set_index("urn")

    guidelines = get_human_guidelines()

    st.session_state.table_data = {}
    for _, row in eval_result.iterrows():
        st.session_state.table_data[row["urn"]] = {
            "description": row["description"],
            "deployment": row["deployment"],
            "data": row["entity_info"],
            "notes": row.get("notes", ""),
            "ai_eval_results": row_to_ai_judge_verdict(
                ai_eval_result_indexed.loc[row["urn"]]
            ),
            "guidelines": guidelines.get(row["urn"]),
        }

    # populate human_eval_results in annotations
    if human_eval_result is not None:
        prefill_annotations(human_eval_result, guidelines)
    else:
        print(
            "No matching human eval results found, trying to load latest human eval results"
        )
        human_eval_result = get_latest_human_eval_result_or_none()
        if human_eval_result is not None:
            prefill_annotations(human_eval_result, guidelines)

    try:
        st.session_state.deployment_details = {
            deployment["deployment"]: deployment["detail"]
            for deployment in get_deployment_details()
        }
    except Exception as e:
        print(f"Error getting deployment details: {e}")
        st.session_state.deployment_details = {}


def extract_table_annotation(current_table_name):
    verdict = HumanJudgeVerdict.parse_obj(
        {
            metric_name: JudgedMetricValue.parse_obj(
                {
                    "value": st.session_state[f"{metric_name}_value"],
                    "guidelines": st.session_state[f"{metric_name}_guidelines"],
                }
            )
            for metric_name in METRIC_NAMES
        }
    )
    # Store the annotation with table information
    annotation = {
        "table_name": current_table_name,
        "verdict": verdict.dict(),
    }

    return annotation


def log_mlflow_run_with_human_annotations(annotations):
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
        for prediction, target in zip(predictions, targets, strict=False):
            judged_metric = getattr(
                common_eval_fn(prediction, json.dumps(target)), metric
            )
            if judged_metric is not None:
                scores.append(judged_metric.bool_value())
                justifications.append(judged_metric.guidelines)
            else:
                scores.append(None)
                justifications.append(None)

        return mlflow.metrics.MetricValue(
            scores=scores,
            justifications=justifications,
            aggregate_results={
                "pass_percentage": 100
                * len(list(filter(lambda x: x, scores)))
                / len(scores)
                if len(scores) > 0
                else 0
            },
        )

    def overall_score_eval_fn(predictions, targets):
        scores = []
        justifications = []
        for prediction, target in zip(predictions, targets, strict=False):
            judged_metric = get_overall_score(
                common_eval_fn(prediction, json.dumps(target))
            )
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

    metric_overall_score = make_overall_score_metric()

    new_run_name = get_human_annotation_run_name(input_run_name)

    with mlflow.start_run(
        experiment_id=st.session_state.input_experiment_id,
        run_name=new_run_name,
        tags={
            "evaluation_type": "human",
        },
    ) as new_run:
        eval_results = get_eval_table(st.session_state.input_run_id)
        mlflow.evaluate(
            data=eval_results,
            predictions="description",
            evaluators="default",
            targets="urn",
            extra_metrics=[
                *[make_custom_metric(metric_name) for metric_name in METRIC_NAMES],
                metric_overall_score,
            ],
        )
        run_url = f"{mlflow.get_tracking_uri()}/#/experiments/{new_run.info.experiment_id}/runs/{new_run.info.run_id}"
        st.session_state.textarea_content = (
            f"Successfully logged all annotations ! [Annotations Run URL]({run_url})"
        )


def log_results():
    # log mlflow run with annotations
    log_mlflow_run_with_human_annotations(st.session_state.annotations)


# --- Callback Functions ---
def update_table_index():
    st.session_state.human_annotations_expander = False
    current_table_index = table_display_names.index(st.session_state.table_selector)
    current_table = table_names[current_table_index]

    show_table(current_table)


def skip_table():
    current_table_index = table_display_names.index(st.session_state.table_selector)
    st.session_state.table_selector = table_display_names[current_table_index + 1]
    update_table_index()


def add_table_annotations():
    current_table_index = table_display_names.index(st.session_state.table_selector)
    current_table = table_names[current_table_index]
    # Create a HumanJudgeVerdict instance with the current annotations
    annotation = extract_table_annotation(current_table)

    st.session_state.annotations[current_table] = annotation
    table_metric_guidelines = _get_edited_guidelines(current_table)

    # This will update the guidelines file
    update_table_guidelines(table_metric_guidelines)

    # instead of reading from file, we are using the updated guidelines
    st.session_state.table_data[current_table]["guidelines"] = table_metric_guidelines

    if current_table_index < len(table_names) - 1:
        st.session_state.table_selector = table_display_names[current_table_index + 1]
        update_table_index()


def done():
    # add_table_annotations()
    log_results()
    st.write("All annotations logged!")


def run_ai_judge(metric_name):
    """Callback to rerun AI judge for a specific metric"""
    st.session_state.human_annotations_expander = True
    current_table_index = table_display_names.index(st.session_state.table_selector)
    current_table = table_names[current_table_index]

    table_metric_guidelines = _get_edited_guidelines(current_table)

    with mlflow.start_run(
        parent_run_id=st.session_state.input_run_id,
        run_name=f"rerun_{metric_name}",
    ):
        llm_judge_verdict = llm_judge_common_eval_fn(
            st.session_state.table_data[current_table]["description"],
            json.dumps(st.session_state.table_data[current_table]["data"]),
            table_metric_guidelines.json(),
        )

    if (
        getattr(llm_judge_verdict, metric_name) is not None
        and getattr(llm_judge_verdict, metric_name).value is not None
    ):
        st.session_state[f"{metric_name}_ai_value"] = getattr(
            llm_judge_verdict, metric_name
        ).value
        st.session_state[f"{metric_name}_ai_reasoning"] = (
            getattr(llm_judge_verdict, metric_name).reasoning or ""
        )
        st.session_state[f"{metric_name}_ai_rerun_msg"] = ""
    else:
        st.session_state[f"{metric_name}_ai_rerun_msg"] = "Failed to rerun AI judge"


def _get_edited_guidelines(current_table: str) -> HumanGuidelines:
    return HumanGuidelines.parse_obj(
        {
            "urn": current_table,
            "deployment": st.session_state.table_data[current_table]["deployment"],
            "guidelines": {
                metric_name: st.session_state[f"{metric_name}_guidelines"].split("\n")
                for metric_name in METRIC_NAMES
            },
        }
    )


def get_additional_info_for_table(current_table: str) -> str:
    details = []
    notes = st.session_state.table_data[current_table]["notes"]
    if (
        st.session_state.table_data[current_table]["deployment"]
        in st.session_state.deployment_details
    ):
        details.append(
            f"{st.session_state.deployment_details[st.session_state.table_data[current_table]['deployment']]}"
        )
    if notes:
        details.append(f"{notes}")

    return "\n\r".join(details)


# --- Layout ---
input_run = get_input_run(input_run_name)
eval_results = get_eval_table(input_run.info.run_id)


if "initialized" not in st.session_state:
    st.session_state.input_run_id = input_run.info.run_id
    st.session_state.input_experiment_id = input_run.info.experiment_id
    init_session_state_with_previous_annotations(input_run_name, eval_results)
    st.session_state.initialized = True
    st.session_state.first_load = True

table_names = list(st.session_state.table_data.keys())
table_display_names = [
    f"{st.session_state.table_data[table_name]['deployment']} - {table_name}"
    for table_name in table_names
]


# Custom CSS to increase max-width
st.markdown(
    """
    <style>
    #MainMenu {visibility: hidden;}
   .stMainBlockContainer {
        max-width: 1024px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Table Description Evaluation Tool")

selected_table = st.selectbox(
    "Select a table to review",
    options=table_display_names,
    key="table_selector",
    on_change=update_table_index,
)

# Call update_table_index on first load
if st.session_state.get("first_load", False):
    update_table_index()
    st.session_state.first_load = False

table_index = table_display_names.index(st.session_state.table_selector)
current_table = table_names[table_index]


input_run_url = f"{mlflow.get_tracking_uri()}/#/experiments/{input_run.info.experiment_id}/runs/{input_run.info.run_id}"
link_columns = st.columns([1, 1, 1])
with link_columns[0]:
    st.text(f"Table {table_index + 1} of {len(table_display_names)}")
with link_columns[1]:
    st.markdown(
        f"[View in DataHub](https://{st.session_state.table_data[current_table]['deployment']}.acryl.io/login?redirect_uri=/dataset/{current_table})"
    )
with link_columns[2]:
    st.markdown(f"[View in MLFlow]({input_run_url})")

if st.session_state.annotations.get(current_table):
    st.markdown(":green[This table has already been annotated.]")


st.info(
    get_additional_info_for_table(current_table),
)
# Button with callback
b1, b2, b3 = st.columns([1, 1, 2])
with b1:
    st.button(
        "**Submit and Go to Next**",
        key="submit_and_next1",
        on_click=add_table_annotations,
    )
with b2:
    st.button("**Skip and Go to Next**", key="skip_and_next1", on_click=skip_table)

with st.expander("**Table Info and Column Infos**", expanded=True):
    info = ExtractedTableInfo.parse_obj(
        st.session_state.table_data[current_table]["data"]
    )
    table_info, column_info = transform_table_info_for_llm(info)
    st.json(table_info.dict(exclude_none=True), expanded=False)
    st.json(
        {k: col.dict(exclude_none=True) for k, col in column_info.items()},
        expanded=False,
    )


with st.expander("**Generated Description**", expanded=True):
    tab1, tab2 = st.tabs(["Formatted", "Raw"])
    with tab1:
        st.markdown(st.session_state.table_data[current_table]["description"])
    with tab2:
        st.text(st.session_state.table_data[current_table]["description"])


# NOTE: This expanded = False expander is a hack to scroll to almost top after next button is clicked
# This does not work so well.
expanded = st.session_state.human_annotations_expander

with st.expander(
    "**Add Human Annotations**",
    expanded=expanded,
):
    for i, metric_name in enumerate(METRIC_NAMES):
        with st.container(border=True):
            st.subheader(metric_name)
            st.text(METRICS_CONFIG[i].definition)
            c1, c2 = st.columns([2, 2])
            with c1:
                st.radio(
                    "Human Evaluation:",
                    ["pass", "fail"],
                    key=f"{metric_name}_value",
                )
            with c2:
                ai_col1, ai_col2 = st.columns([1, 2])
                with ai_col1:
                    st.radio(
                        "AI Evaluation:",
                        ["pass", "fail"],
                        key=f"{metric_name}_ai_value",
                        disabled=True,
                    )
                with ai_col2:
                    st.text_area(
                        "AI Reasoning:",
                        key=f"{metric_name}_ai_reasoning",
                        disabled=True,
                    )

            guidelines_col1, guidelines_col2 = st.columns([3, 1])
            with guidelines_col1:
                st.text_area(
                    "Human Guidelines:",
                    key=f"{metric_name}_guidelines",
                )
            with guidelines_col2:
                st.container().markdown(
                    """
                    <style>
                    .button-container {
                        display: flex;
                        flex-direction: column;
                        justify-content: flex-end;
                        height: 100%;
                    }
                    </style>
                    <div class="button-container">
                    """,
                    unsafe_allow_html=True,
                )
                st.text(st.session_state.get(f"{metric_name}_ai_rerun_msg", ""))
                st.button(
                    "Rerun AI Judge",
                    key=f"rerun_{metric_name}",
                    on_click=run_ai_judge,
                    args=(metric_name,),
                )
                st.markdown("</div>", unsafe_allow_html=True)
    # Button with callback, repeated from above
    b1, b2, b3 = st.columns([1, 1, 2])
    with b1:
        st.button(
            "**Submit and Go to Next**",
            key="submit_and_next2",
            on_click=add_table_annotations,
        )
    with b2:
        st.button("**Skip and Go to Next**", key="skip_and_next2", on_click=skip_table)


st.write(f"Annotated {len(st.session_state.annotations)} tables so far")
with st.expander("Annotations Preview", expanded=False):
    st.json(st.session_state.annotations)

st.button("**Submit and Finish**", on_click=done)
# add editable comment section
if st.session_state.get("textarea_content"):
    st.markdown(st.session_state.textarea_content)
