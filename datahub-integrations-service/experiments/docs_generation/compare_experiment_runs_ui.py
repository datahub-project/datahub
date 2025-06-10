import argparse
import os

import dotenv
import mlflow
import mlflow.entities
import pandas as pd
import streamlit as st
from eval_common import METRIC_NAMES, METRICS_CONFIG
from mlflow_common import (
    get_ai_eval_result_or_none,
    get_run_or_fail,
    load_eval_table,
)

dotenv.load_dotenv()

# Create an argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "--run-name-1", type=str, required=True, help="First MLflow run name to compare"
)
parser.add_argument(
    "--run-name-2", type=str, required=True, help="Second MLflow run name to compare"
)

# Parse the command-line arguments
args = parser.parse_args()
EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME")
mlflow.set_experiment(EXPERIMENT_NAME)

# Access the run_ids
input_run_name_1 = args.run_name_1
input_run_name_2 = args.run_name_2


# --- Helper Functions ---
@st.cache_data
def get_input_run(input_run_name: str) -> mlflow.entities.Run:
    return get_run_or_fail(input_run_name)


@st.cache_data
def get_eval_table(input_run_id: str) -> pd.DataFrame:
    return load_eval_table(input_run_id)


def init_session_state() -> None:
    """Initialize session state with data from both runs"""
    if "initialized" not in st.session_state:
        # Get run data
        run1 = get_input_run(input_run_name_1)
        run2 = get_input_run(input_run_name_2)

        # Get evaluation results
        eval_results1 = get_eval_table(run1.info.run_id)
        eval_results2 = get_eval_table(run2.info.run_id)

        # Get AI evaluation results
        ai_eval_results1 = get_ai_eval_result_or_none(input_run_name_1)
        ai_eval_results2 = get_ai_eval_result_or_none(input_run_name_2)

        assert ai_eval_results1 is not None and ai_eval_results2 is not None

        # Index AI results by URN
        ai_eval_results1_indexed = ai_eval_results1.set_index("urn")
        ai_eval_results2_indexed = ai_eval_results2.set_index("urn")

        # Store data in session state
        st.session_state.run1_id = run1.info.run_id
        st.session_state.run2_id = run2.info.run_id
        st.session_state.run1_experiment_id = run1.info.experiment_id
        st.session_state.run2_experiment_id = run2.info.experiment_id

        # Store table data
        st.session_state.table_data = {}
        for _, row in eval_results1.iterrows():
            urn = row["urn"]
            st.session_state.table_data[urn] = {
                "description_run1": row["description"],
                "description_run2": eval_results2.loc[
                    eval_results2["urn"] == urn, "description"
                ].iloc[0]
                if urn in eval_results2["urn"].values
                else "",
                "deployment": row["deployment"],
                "data": row["entity_info"],
                "ai_eval_results_run1": ai_eval_results1_indexed.loc[urn]
                if urn in ai_eval_results1_indexed.index
                else None,
                "ai_eval_results_run2": ai_eval_results2_indexed.loc[urn]
                if urn in ai_eval_results2_indexed.index
                else None,
            }

        st.session_state.initialized = True
        st.session_state.first_load = True


def get_additional_info_for_table(current_table: str) -> str:
    details = []
    notes = st.session_state.table_data[current_table]["ai_eval_results_run1"]["notes"]
    if notes:
        details.append(f"{notes}")

    return "\n\r".join(details)


# --- Main UI ---
# Initialize session state
init_session_state()

# Get table names and display names
table_names = list(st.session_state.table_data.keys())
table_display_names = [
    f"{st.session_state.table_data[table_name]['deployment']} - {table_name}"
    for table_name in table_names
]

# Custom CSS to increase max-width and add some spacing
st.markdown(
    """
    <style>
    #MainMenu {visibility: hidden;}
    .stMainBlockContainer {
        max-width: 1536px;
    }
    .stContainer {
        margin-bottom: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Table Description Comparison Tool")

# Table selector
selected_table = st.selectbox(
    "Select a table to compare",
    options=table_display_names,
    key="table_selector",
)

# Get current table info
table_index = table_display_names.index(st.session_state.table_selector)
current_table = table_names[table_index]

# Display notes if available
st.info(
    get_additional_info_for_table(current_table),
)

# Create fixed containers for descriptions and metrics
columns = st.columns([1, 1])
with columns[0]:
    desc_container1 = st.container(key="desc_container1")
with columns[1]:
    desc_container2 = st.container(key="desc_container2")
metrics_container = st.container(key="metrics_container")
with desc_container1:
    st.subheader(f"Run 1: {input_run_name_1}")
    st.write(
        "Table DescriptionOverall Score:",
        st.session_state.table_data[current_table]["ai_eval_results_run1"][
            "overall_score/score"
        ],
    )
    with st.expander("Description", expanded=True):
        tab1, tab2 = st.tabs(["Formatted", "Raw"])
        with tab1:
            st.markdown(st.session_state.table_data[current_table]["description_run1"])
        with tab2:
            st.text(st.session_state.table_data[current_table]["description_run1"])

with desc_container2:
    st.subheader(f"Run 2: {input_run_name_2}")
    st.write(
        "Overall Score:",
        st.session_state.table_data[current_table]["ai_eval_results_run2"][
            "overall_score/score"
        ],
    )
    with st.expander("Description", expanded=True):
        tab1, tab2 = st.tabs(["Formatted", "Raw"])
        with tab1:
            st.markdown(st.session_state.table_data[current_table]["description_run2"])
        with tab2:
            st.text(st.session_state.table_data[current_table]["description_run2"])

# Update metrics container
with metrics_container:
    st.subheader("Metric Scores and Reasoning")
    for metric_name in METRIC_NAMES:
        with st.container(border=True):
            st.subheader(metric_name)
            st.text(METRICS_CONFIG[METRIC_NAMES.index(metric_name)].definition)

            col1, col2 = st.columns(2)
            with col1:
                if (
                    st.session_state.table_data[current_table]["ai_eval_results_run1"]
                    is not None
                ):
                    st.write(
                        "Run 1 Score:",
                        st.session_state.table_data[current_table][
                            "ai_eval_results_run1"
                        ][f"{metric_name}/score"],
                    )
                    st.write(
                        "Run 1 Reasoning:",
                        st.session_state.table_data[current_table][
                            "ai_eval_results_run1"
                        ][f"{metric_name}/justification"],
                    )

            with col2:
                if (
                    st.session_state.table_data[current_table]["ai_eval_results_run2"]
                    is not None
                ):
                    st.write(
                        "Run 2 Score:",
                        st.session_state.table_data[current_table][
                            "ai_eval_results_run2"
                        ][f"{metric_name}/score"],
                    )
                    st.write(
                        "Run 2 Reasoning:",
                        st.session_state.table_data[current_table][
                            "ai_eval_results_run2"
                        ][f"{metric_name}/justification"],
                    )
