import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import mlflow
import mlflow.utils
import mlflow.utils.databricks_utils
import pandas as pd
import pydantic
import streamlit as st
from mlflow import entities as mlflow_entities
from st_aggrid import AgGrid, GridOptionsBuilder

from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.experimentation.chatbot.chatbot import (
    ExpectedToolCall,
    Prompt,
    prompts,
    reload_prompt,
    update_prompt_expected_tool_calls,
    update_prompt_guidelines,
    update_prompt_tags,
)
from datahub_integrations.experimentation.chatbot.judge import (
    LLMJudgeResponse,
    ToolCallValidationResult,
    chatbot_llm_judge_evaluation,
    validate_expected_tool_calls,
)
from datahub_integrations.experimentation.chatbot.st_chat_history import st_chat_history
from datahub_integrations.experimentation.mlflow_utils import (
    get_most_recent_run_for_expt,
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
    return get_most_recent_run_for_expt(EXPERIMENT_NAME)

@st.cache_data()
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
            try:
                history = ChatHistory.model_validate_json(raw_history)
            except pydantic.ValidationError as e:
                st.error(f"Error loading history for {prompt_id}: {e}")
                history = None

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

        # Evaluate expected tool calls if they exist
        if prompt.expected_tool_calls and history:
            tool_call_evaluation = validate_expected_tool_calls(
                history, prompt.expected_tool_calls
            )
        else:
            tool_call_evaluation = ToolCallValidationResult(
                is_valid=False,
                justification="No chat history or no expected tool calls to evaluate",
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
            "Tool Calls Pass": {
                True: "✅",
                False: "❌",
                None: "❓",
            }[tool_call_evaluation.is_valid],
            "Instance": prompt.instance,
            "Tags": ", ".join(prompt.tags) if prompt.tags else "",
            "Raw Data": {
                "prompt": prompt,
                "history": history,
                "raw_history": raw_history,
                "evaluation": evaluation,
                "tool_call_evaluation": tool_call_evaluation,
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

    # Configure AgGrid with minimal settings
    display_data = table_data.copy().drop(columns=["Raw Data"])[
        [
            "Prompt ID",
            "Prompt",
            "Response",
            "Pass",
            "Justification",
            "Guidelines",
            "Tool Calls Pass",
            "Instance",
            "Tags",
        ]
    ]
    gb = GridOptionsBuilder.from_dataframe(display_data)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    gb.configure_default_column(
        filter=True,
        maxWidth=300,
    )

    grid_response = AgGrid(display_data, gridOptions=gb.build())

    # Show selected prompt details
    selected_rows = grid_response["selected_rows"]
    if selected_rows is not None and not selected_rows.empty:
        # Get the selected row data from the grid response
        selected_data = selected_rows.iloc[0]
        # Find the corresponding row in the original table_data using Prompt ID
        selected_row = table_data[
            table_data["Prompt ID"] == selected_data["Prompt ID"]
        ].iloc[0]
        prompt_id = selected_row["Prompt ID"]
        st.text(f"Prompt ID: {prompt_id}")

        # Get prompt from file (updated data) instead of table data
        prompt: Prompt = reload_prompt(prompt_id)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Conversation History")
            if history := selected_row["Raw Data"]["history"]:
                show_thinking = st.toggle("Show Thinking", value=True)
                st_chat_history(history, show_thinking=show_thinking)
            else:
                st.markdown("No conversation history found")
                st.code(selected_row["Raw Data"]["raw_history"], language="json")

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
                print(f"Guidelines updated for prompt {prompt.id}")
                st.success("Guidelines updated!")

            if new_guidelines != prompt.response_guidelines:
                st.markdown("### Updated Evaluation")
                new_evaluation = chatbot_llm_judge_evaluation(
                    message=prompt.message,
                    response=selected_row["Response"],
                    guidelines=new_guidelines,
                )
                st.json(new_evaluation.model_dump())
            else:
                st.markdown("### Current Evaluation")
                old_evaluation: LLMJudgeResponse = selected_row["Raw Data"][
                    "evaluation"
                ]
                st.json(old_evaluation.model_dump())

#            # Expected Tool Calls Section
#            st.markdown("### Expected Tool Calls")
#            current_expected_calls = prompt.expected_tool_calls or []
#
#            # Create a text area for editing expected tool calls as JSON
#            current_calls_json = (
#                json.dumps(
#                    [call.model_dump() for call in current_expected_calls],
#                    indent=2,
#                )
#                if current_expected_calls
#                else "[]"
#            )
#
#            new_calls_json = st.text_area(
#                "Edit Expected Tool Calls (JSON)",
#                key=f"expected-calls-{prompt_id}",
#                value=current_calls_json,
#                height=150,
#                help='Enter expected tool calls as JSON array. Example: [{"tool_name": "search", "tool_input": {"query": "*"}}]',
#                label_visibility="visible",
#            )
#
#            try:
#                new_calls_data = json.loads(new_calls_json)
#                new_expected_calls = (
#                    [
#                        ExpectedToolCall(
#                            tool_name=call["tool_name"], tool_input=call["tool_input"]
#                        )
#                        for call in new_calls_data
#                    ]
#                    if new_calls_data
#                    else None
#                )
#
#                if st.button(
#                    "Update Expected Tool Calls",
#                    disabled=new_calls_json == current_calls_json,
#                ):
#                    update_prompt_expected_tool_calls(prompt.id, new_expected_calls)
#                    print(f"Expected tool calls updated for prompt {prompt.id}")
#                    st.success("Expected tool calls updated!")
#
#                # Show updated evaluation if JSON has changed
#                if new_calls_json != current_calls_json and history:
#                    st.markdown("### Updated Tool Call Evaluation")
#                    updated_tool_eval = validate_expected_tool_calls(
#                        history, new_expected_calls or []
#                    )
#                    st.json(
#                        {
#                            "is_valid": updated_tool_eval.is_valid,
#                            "justification": updated_tool_eval.justification,
#                        }
#                    )
#                else:
#                    st.markdown("### Current Tool Call Evaluation")
#                    old_tool_eval = selected_row["Raw Data"]["tool_call_evaluation"]
#                    st.json(
#                        {
#                            "is_valid": old_tool_eval.is_valid,
#                            "justification": old_tool_eval.justification,
#                        }
#                    )
#
#            except json.JSONDecodeError as e:
#                st.error(f"Invalid JSON: {e}")
#            except (KeyError, TypeError) as e:
#                st.error(f"Invalid expected tool calls format: {e}")
#                st.info(
#                    'Expected format: [{"tool_name": "tool_name", "tool_input": {"param": "value"}}]'
#                )

            # Tags Section
            st.markdown("### Tags")
            current_tags = prompt.tags or []
            current_tags_str = ", ".join(current_tags) if current_tags else ""

            new_tags_str = st.text_input(
                "Edit Tags",
                key=f"tags-{prompt_id}",
                value=current_tags_str,
                help="Enter tags as a comma-separated list (e.g., 'tag1, tag2, tag3')",
                label_visibility="visible",
            )

            # Parse the comma-separated string into a list
            new_tags = [
                tag.strip() for tag in new_tags_str.split(",") if tag.strip()
            ] if new_tags_str else []

            if st.button(
                "Update Tags",
                disabled=new_tags == current_tags,
            ):
                update_prompt_tags(prompt.id, new_tags if new_tags else None)
                print(f"Tags updated for prompt {prompt.id}")
                st.success("Tags updated!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chat Review Streamlit App")
    parser.add_argument(
        "--run-name",
        type=str,
        help="Optional MLflow run name to load specific run data",
        default=None,
    )
    args = parser.parse_args()
    main(run_name=args.run_name)
