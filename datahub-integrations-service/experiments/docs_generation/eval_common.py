import json
import os
import pathlib
import subprocess
import threading
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    create_model,
    field_validator,
)


class MetricScoringCriteria(BaseModel):
    possible_values: list[str]
    guidelines: list[str]


class MetricConfig(BaseModel):
    name: str
    definition: str
    type: str
    scoring_criteria: MetricScoringCriteria
    alias: Optional[str] = None

    def name_for_ai_annotation(self) -> str:
        return self.alias if self.alias is not None else self.name


class HumanGuidelines(BaseModel):
    urn: str
    deployment: str
    guidelines: Dict[str, List[str]] = Field(
        default_factory=lambda: defaultdict(list)  # type: ignore
    )  # metric name to list of guidelines


class JudgedMetricValue(BaseModel):
    reasoning: Optional[str] = None
    value: Optional[str] = None
    guidelines: Optional[str] = None  # stringified list of guidelines

    def bool_value(self) -> Optional[bool]:
        if self.value is not None:
            return self.value.strip().lower() == "pass"
        else:
            return None

    @field_validator("value", mode="before")
    def value_field_validator(cls, v: Any) -> Optional[str]:
        if v is None or v == "nan" or (isinstance(v, float) and np.isnan(v)):
            return None

        if isinstance(v, bool):
            return "pass" if v else "fail"

        if isinstance(v, (int, float)):
            return "pass" if int(v) == 1 else "fail"

        if isinstance(v, str):
            v_lower = v.lower().strip()
            if v_lower in ("true", "1", "1.0", "pass"):
                return "pass"
            elif v_lower in ("false", "0", "0.0", "fail"):
                return "fail"
        assert v in [
            "pass",
            "fail",
        ], f"Invalid value: {v} for type {type(v)}, should be one of ['pass', 'fail']"
        return v


class IntegerMetricValue(BaseModel):
    value: Optional[int] = 0
    reasoning: Optional[str] = None


def load_eval_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_metric_configs_from_config(config: dict) -> list[MetricConfig]:
    return [MetricConfig.model_validate(metric) for metric in config["metrics"]]


def get_metric_names_from_config(config: dict) -> list[str]:
    return [metric["name"] for metric in config["metrics"]]


def get_human_annotation_run_name(run_name: str) -> str:
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"human_annotations_{original_run_name}"


def get_ai_annotation_run_name(run_name: str) -> str:
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"ai_annotations_{original_run_name}"


def get_ai_judge_eval_run_name(run_name: str) -> str:
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"ai_judge_eval_{original_run_name}"


def get_original_expt_run_name(run_name: str) -> str:
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return original_run_name


METRICS_CONFIG = get_metric_configs_from_config(
    load_eval_config("eval_config/metrics_config.yaml")
)
METRIC_NAMES = [metric.name for metric in METRICS_CONFIG]


AIJudgeVerdict = create_model(
    "AIJudgeVerdict",
    __config__=ConfigDict(
        populate_by_name=True,
    ),
    **{
        metric.name: (
            Optional[JudgedMetricValue],
            # This is in order to support different metric name only for AI judge.
            # Ideally we can also simply rename the metric but we don't want to lose
            # earlier annotations with different metric name, so this is workaround.
            (
                Field(default=None, alias=metric.alias)
                if metric.alias
                else Field(default=None)
            ),
        )
        for metric in METRICS_CONFIG
    },
    
)

HumanJudgeVerdict = create_model(
    "HumanJudgeVerdict",
    **{
        metric_name: (Optional[JudgedMetricValue], None) for metric_name in METRIC_NAMES
    },
)


def get_overall_score(
    verdict: Union["AIJudgeVerdict", "HumanJudgeVerdict"],
) -> Optional[IntegerMetricValue]:
    # Overall score is the sum of all the metrics that are passed (True = 1, False = 0)
    # In future, we can add weights to each metric and calculate the overall score
    score = 0
    for metric in METRIC_NAMES:
        if (
            getattr(verdict, metric) is not None
            and getattr(verdict, metric).value is not None
        ):
            score += 1 if getattr(verdict, metric).bool_value() else 0
    if all(
        getattr(verdict, metric) is None or getattr(verdict, metric).value is None
        for metric in METRIC_NAMES
    ):
        # Setting reasoning to None to indicate that no metrics were judged.
        # This hack is used during overall score evaluation to keep only judged entries.
        return IntegerMetricValue(value=score, reasoning=None)
    else:
        return IntegerMetricValue(
            value=score,
            reasoning=f"Overall score is {score} out of {len(METRIC_NAMES)}",
        )


def get_human_guidelines() -> Dict[str, HumanGuidelines]:
    guidelines = load_eval_config("eval_config/eval_set_guidelines.yaml")
    guidelines_dict = {
        entry["urn"]: HumanGuidelines.model_validate(entry)
        for entry in guidelines["config"]
    }
    return guidelines_dict


# Create a lock for update_table_guidelines
_update_guidelines_lock = threading.Lock()


def update_table_guidelines(table_metric_guidelines: HumanGuidelines) -> None:
    with _update_guidelines_lock:
        guidelines_path = "eval_config/eval_set_guidelines.yaml"
        guidelines = load_eval_config(guidelines_path)
        guidelines_dict = {
            entry["urn"]: HumanGuidelines.model_validate(entry)
            for entry in guidelines["config"]
        }
        # update
        guidelines_dict[table_metric_guidelines.urn] = table_metric_guidelines
        with open(guidelines_path, "w") as f:
            yaml.dump({"config": [g.model_dump() for g in guidelines_dict.values()]}, f)


def update_guidelines_file(guidelines_dict: Dict[str, HumanGuidelines]) -> None:
    with _update_guidelines_lock:
        guidelines_path = "eval_config/eval_set_guidelines.yaml"
        with open(guidelines_path, "w") as f:
            yaml.dump({"config": list(guidelines_dict.values())}, f)


def get_deployment_details() -> List[Dict[str, str]]:
    return json.load(open("eval_config/deployment_details.json"))


def execute_notebook_save_as_html(
    notebook_path: pathlib.Path, output_dir: pathlib.Path, params: Dict[str, str]
) -> str:
    """
    Executes a Jupyter Notebook and saves the executed version as HTML.

    Args:
        notebook_path (str): The path to the Jupyter Notebook file (.ipynb).
        output_dir (str): The directory to save the executed notebook.

    Returns:
        str: The path to the successfully executed HTML notebook.

    Raises:
        FileNotFoundError: If the 'jupyter' command is not found.
        subprocess.CalledProcessError: If the notebook execution fails.
        Exception: For any other unexpected errors during execution.
    """
    notebook_filename = os.path.basename(notebook_path)
    notebook_name_without_ext, _ = os.path.splitext(notebook_filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    executed_notebook_filename = (
        f"{notebook_name_without_ext}_executed_{timestamp}.ipynb"
    )
    executed_notebook_path = os.path.join(output_dir, executed_notebook_filename)
    executed_notebook_filename_html = (
        f"{notebook_name_without_ext}_executed_{timestamp}.html"
    )
    executed_notebook_path_html = os.path.join(
        output_dir, executed_notebook_filename_html
    )

    os.makedirs(output_dir, exist_ok=True)

    print(f"Executing notebook: {notebook_path}")

    param_options = []
    for k, v in params.items():
        param_options.append("-p")
        param_options.append(k)
        param_options.append(v)
    papermill_command = [
        # Execute the notebook with Papermill and then convert the result to HTML
        "papermill",
        str(notebook_path),
        str(executed_notebook_path),
        *param_options,
    ]
    jupyter_command = [
        "jupyter",
        "nbconvert",
        "--to",
        "html",
        "--no-input",
        str(executed_notebook_path),
        "--output",
        str(executed_notebook_path_html),
    ]
    subprocess.run(papermill_command, check=True)
    subprocess.run(jupyter_command, check=True)
    print(f"Executed notebook saved to: {executed_notebook_path_html}")
    return executed_notebook_path_html
