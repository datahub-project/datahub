import json
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import mlflow
from mlflow import entities as mlflow_entities
import mlflow.utils
import mlflow.utils.databricks_utils
import pandas as pd
import streamlit as st

from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.experimentation.chatbot import (
    Prompt,
    prompts,
    update_prompt_guidelines,
)
from datahub_integrations.experimentation.st_chat_history import st_chat_history
from datahub_integrations.experimentation.judge import (
    LLMJudgeResponse,
    chatbot_llm_judge_evaluation,
)

st.set_page_config(layout="wide")

# Constants
EXPERIMENT_NAME = "Chatbot"


@st.cache_data(persist=True)
def load_run_data(run_id: str) -> pd.DataFrame:
    """Load and cache the dataset from MLflow for a given run name."""
    return mlflow.load_table(artifact_file="eval_results_table.json", run_ids=[run_id])


@st.cache_data()
def get_most_recent_run() -> mlflow_entities.Run:
    """Get the most recent MLflow run name."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        order_by=["start_time DESC"],
        max_results=1,
        output_format="list",
    )
    assert isinstance(runs, list)
    if not runs:
        raise ValueError("No runs found in experiment")
    return runs[0]


def get_run_or_fail(run_name: str) -> mlflow_entities.Run:
    """Get MLflow run or fail if not found."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}'",
        output_format="list",
    )
    assert isinstance(runs, list)
    if not runs:
        raise ValueError(f"Run {run_name} not found")
    return runs[0]


def get_mlflow_run_url(run: mlflow_entities.Run) -> str:
    """Generate MLflow UI URL for the run."""
    # host_url = mlflow.utils.databricks_utils.get_workspace_url()
    host_url = "http://localhost:9090"
    return f"{host_url}/#/experiments/{run.info.experiment_id}/runs/{run.info.run_id}"


def format_table_data(run_data: pd.DataFrame) -> pd.DataFrame:
    """Format the run data for table display."""

    # Create a mapping of prompt IDs to their original prompts
    prompt_map = {p.id: p for p in prompts}

    def _process_row(row: pd.Series) -> dict:
        prompt_id = row["prompt_id"]
        if prompt_id not in prompt_map:
            raise ValueError(f"Prompt {prompt_id} not found in prompt map")

        prompt = prompt_map[prompt_id]

        history = None
        if raw_history := row["history"]:
            history = ChatHistory.parse_raw(raw_history)

        next_message = None
        response = None
        if raw_next_message := row["next_message"]:
            next_message = json.loads(raw_next_message)
            response = next_message["text"]

        if response is not None:
            evaluation = chatbot_llm_judge_evaluation(
                message=prompt.message,
                response=response,
                guidelines=prompt.response_guidelines or "",
            )
        else:
            evaluation = LLMJudgeResponse(
                choice=None,
                justification="No response from model",
            )

        return {
            "Prompt ID": prompt_id,
            "Prompt": prompt.message,
            "Response": response,
            "Pass": {
                True: "✅",
                False: "❌",
                None: "❓",
            }[evaluation.choice],
            "Justification": evaluation.justification,
            "Guidelines": prompt.response_guidelines or "",
            "Instance": prompt.instance,
            "Raw Data": {
                "prompt": prompt,
                "history": history,
                "evaluation": evaluation,
            },
        }

    with ThreadPoolExecutor() as executor:
        table_data = list(
            executor.map(_process_row, (row for _, row in run_data.iterrows()))
        )

    if not table_data:
        st.warning("No valid chat histories found in the run data.")
        return pd.DataFrame()

    return pd.DataFrame(table_data)


def main(run_name: Optional[str] = None):
    """Main Streamlit app function."""

    st.title("Chat Review")

    if run_name is None:
        run = get_most_recent_run()

        st.button("Refresh", on_click=get_most_recent_run.clear)
    else:
        run = get_run_or_fail(run_name)

    st.markdown(f"MLflow Run: [{run.info.run_name}]({get_mlflow_run_url(run)})")

    # Load and display data
    run_data = load_run_data(run.info.run_id)
    table_data = format_table_data(run_data)

    event = st.dataframe(
        table_data.copy().drop(columns=["Raw Data"]),
        column_order=[
            "Prompt ID",
            "Prompt",
            "Response",
            "Pass",
            "Justification",
            "Guidelines",
            "Instance",
        ],
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
    )

    # Show selected prompt details
    if event.selection["rows"]:
        selected_row = table_data.iloc[event.selection.rows[0]]
        prompt_id = selected_row["Prompt ID"]
        st.text(f"Prompt ID: {prompt_id}")

        prompt: Prompt = selected_row["Raw Data"]["prompt"]

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Conversation History")
            show_thinking = st.toggle("Show Thinking", value=True)
            st_chat_history(
                selected_row["Raw Data"]["history"], show_thinking=show_thinking
            )

        with col2:
            st.markdown("### Response Guidelines")
            new_guidelines = st.text_area(
                "Edit Guidelines",
                key=f"guidelines-{prompt_id}",
                value=prompt.response_guidelines or "",
                height=200,
            )

            if st.button(
                "Update Guidelines",
                disabled=new_guidelines == prompt.response_guidelines,
            ):
                update_prompt_guidelines(prompt.id, new_guidelines)

            if new_guidelines != prompt.response_guidelines:
                st.markdown("### Updated Evaluation")
                new_evaluation = chatbot_llm_judge_evaluation(
                    message=prompt.message,
                    response=selected_row["Response"],
                    guidelines=new_guidelines,
                )
                st.json(new_evaluation.dict())
            else:
                st.markdown("### Current Evaluation")
                st.json(selected_row["Raw Data"]["evaluation"].dict())


main()
