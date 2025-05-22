import functools
import glob
import json
import os
import re
from typing import Dict, List, Optional

import asyncer
import dotenv
import mlflow
import mlflow.bedrock
import mlflow.metrics
import numpy as np
import tenacity
import typer
from eval_common import (
    METRICS_CONFIG,
    AIJudgeVerdict,
    HumanGuidelines,
    JudgedMetricValue,
    get_ai_annotation_run_name,
    get_human_guidelines,
    get_overall_score,
)
from mlflow_common import get_run_or_fail
from pydantic import BaseModel

from datahub_integrations.gen_ai.bedrock import (
    BedrockModel,
    call_bedrock_llm_with_retry,
)
from datahub_integrations.gen_ai.description_v2 import (
    ColumnMetadataInfo,
    ExtractedTableInfo,
    TableInfo,
    transform_table_info_for_llm,
)

dotenv.load_dotenv()
EXPERIMENT_NAME = os.getenv("DOCS_GENERATION_EXPERIMENT_NAME")
mlflow.set_experiment(EXPERIMENT_NAME)
mlflow.bedrock.autolog()
AI_JUDGE_MODEL = BedrockModel.CLAUDE_37_SONNET


class FewShotExample(BaseModel):
    table_info: TableInfo
    column_infos: Dict[str, ColumnMetadataInfo]
    description: str
    ai_judge_verdict: "AIJudgeVerdict"

    def pretty_example(self) -> str:
        return f"""\
Provided Information:
Table Info:
{json.dumps(self.table_info.dict(exclude_none=True), indent=4)}
Column Infos:
{json.dumps({k: v.dict(exclude_none=True) for k, v in self.column_infos.items()}, indent=4)}

Generated Description To Evaluate:
{self.description}

Output:
{json.dumps(self.ai_judge_verdict.dict(exclude_none=True, by_alias=True), indent=4)}
"""


@functools.cache
def build_few_shot_examples(limit: int = 3) -> List[FewShotExample]:
    """
    Build few-shot examples for the AI judge by loading examples from JSON files.

    Args:
        run_name: The name of the run (not used in this implementation)
        limit: Maximum number of examples to return

    Returns:
        List of FewShotExample objects
    """

    examples = []
    # Find all ai_judge_examples_*.json files
    example_files = glob.glob("ai_judge_examples_*.json")

    for file_path in example_files[:limit]:
        try:
            with open(file_path, "r") as f:
                example_data = json.load(f)

            # Parse the example data into a FewShotExample
            example = FewShotExample.parse_obj(example_data)
            examples.append(example)

        except Exception as e:
            print(f"Error loading example from {file_path}: {e}")
            continue

    return examples


LLM_JUDGE_PROMPT = """
You are a judge who is tasked with evaluating the quality of auto-generated table description. The description should be based on facts in provided information. You will be given generated table description and facts available about the table. You need to evaluate the quality of the description with respect the pre-defined metrics. Note that you need to provide one or two line reasoning for your evaluation and  "Pass" or "Fail" result for each metric. The reasoning should be concise and to the point and directly point to improvements in description.

Metrics:
{metric_definitions}

Provide your output in the JSON format:

{response_format}

NOTE:
- Include all metric evaluations in the output. The metric values should be strictly either "Pass" or "Fail" and nothing else.
- For Pass value, justification should include critical aspects that can be improved in description. Include concrete improvements that can be made to the description with reference to provided metric definition and provided information.
- For Fail value, justification should include critical aspects that led to failure of the metric.
- Make sure to not include any special characters (like quotes) in the reasoning string.
- Note that description may contain mention to table in form [table_name](datahub urn). This urn may be used to determine data platform, database and schema of table when evaluating metrics.
{examples}


Now your turn
{table_level_guidelines}
Input:

Provided Information:
Table Info:
{table_info}
Column Infos:
{column_infos}

Generated Description To Evaluate:
{table_description}

Output: """

# Additional Evaluation Notes from Human Annotations:
# - we probably need to give it some instructions on how interpet dbt siblings stuff
# - consistency in the output format - use headings or not, multiple vs single paragraph, etc
# - Description should not explain why the table is fact table or dimension table.
# - Do we exclude empty lineage arrays from provided information?
# - Change schema field urns to column name, table name
# - Looker explores and dashboards are mentioned as tables due to absence of the downstream subtypes.
# - Urn Links are probably not behaving correctly on UI

# TODO: include Human eval guidelines here


def parse_llm_judge_output(text) -> AIJudgeVerdict:
    metrics = AIJudgeVerdict()
    match = re.search(r"\{[^}]*\}", text, re.DOTALL)
    if match:
        try:
            return AIJudgeVerdict.parse_obj(json.loads(text))

        except (SyntaxError, ValueError) as e:
            try:
                fixed_text = try_fix_text_for_error(text, e)
                return AIJudgeVerdict.parse_obj(json.loads(fixed_text))
            except (SyntaxError, ValueError):
                print(f"Dictionary can not be parsed. Text:{text}, Error: {e}")

    else:
        print("No dictionary found in the text.")
        return metrics


@tenacity.retry(stop=tenacity.stop_after_attempt(2))
def try_fix_text_for_error(text, e):
    fix_prompt = """
            The following is the text that was returned from the LLM:
            {text}
            Please fix below python error in order to return a valid JSON object.
            Return only the valid JSON object and no other text.
            The output `text` should work when converted to dict using python command `json.loads(text)`
            Error:
            {e}
            Output:
            """.format(text=text, e=e)
    fixed_text = call_bedrock_llm_with_retry(
        prompt=fix_prompt, model=AI_JUDGE_MODEL, max_tokens=5000
    )
    if not fixed_text.startswith("{"):
        json_start_idx = fixed_text.index("{")
        fixed_text = fixed_text[json_start_idx:]
    json.loads(
        fixed_text
    )  # this will raise an error if the text is not a valid JSON object
    return fixed_text


@functools.cache
@mlflow.trace(name="llm_judge_common_eval_fn", span_type="function")
def llm_judge_common_eval_fn(
    table_description: Optional[str],
    entity_info_str: str,
    table_metric_guidelines_str: Optional[str],
) -> AIJudgeVerdict:
    metrics = AIJudgeVerdict()
    metric_alias_map = {
        metric.name: metric.name_for_ai_annotation() for metric in METRICS_CONFIG
    }

    metric_definitions, response_format = gen_dynamic_metric_contents()

    if table_description is None:
        return metrics

    try:
        entity_info = ExtractedTableInfo.parse_raw(entity_info_str)
        mlflow.update_current_trace(
            tags={"tag": "ai_judge", "table_name": entity_info.table_name}
        )

        table_info, column_infos = transform_table_info_for_llm(entity_info)
        try:
            examples = build_few_shot_examples()
            examples_str = "Examples:\n" + "\n".join(
                [example.pretty_example() for example in examples]
            )
        except Exception as e:
            print(f"Error building few shot examples: {e}")
            examples_str = ""

        table_metric_guidelines: Optional[HumanGuidelines] = None
        if table_metric_guidelines_str is not None:
            table_metric_guidelines = HumanGuidelines.parse_raw(
                table_metric_guidelines_str
            )
        if table_metric_guidelines is None or not table_metric_guidelines.guidelines:
            table_level_guidelines = ""
        else:
            metric_guidelines = "\n".join(
                [
                    f"""Metric {metric_alias_map[metric]}:
{guidelines}"""
                    for metric, guidelines in table_metric_guidelines.guidelines.items()
                    if guidelines
                ]
            )
            table_level_guidelines = f"""\
Here are additional guidelines for evaluating description for this table. These guidelines take precedence over provided information as well as generic metric guidelines.
Strictly follow these metric specific guidelines and Fail the metric if the description does not meet the guidelines.
Table Level Guidelines:
{metric_guidelines}"""

        judge_prompt = LLM_JUDGE_PROMPT.format(
            metric_definitions=metric_definitions,
            response_format=response_format,
            examples=examples_str,
            table_description=table_description,
            table_info=json.dumps(table_info.dict(exclude_none=True), indent=4),
            column_infos=json.dumps(
                {k: v.dict(exclude_none=True) for k, v in column_infos.items()},
                indent=4,
            ),
            table_level_guidelines=table_level_guidelines,
        )

        judge_verdict = call_bedrock_llm_with_retry(
            prompt=judge_prompt, model=AI_JUDGE_MODEL, max_tokens=5000
        )
        metrics = parse_llm_judge_output(judge_verdict)
    except Exception as e:
        if isinstance(e, tenacity.RetryError):
            e = e.last_attempt.exception()
        print(f"Error evaluating metric: {e}")
        return metrics

    return metrics


def gen_dynamic_metric_contents():
    metric_definitions = "\n".join(
        [
            f"""Metric {metric.name_for_ai_annotation()}:
{metric.definition}
"""
            for metric in METRICS_CONFIG
        ]
    )

    response_format = json.dumps(
        {
            metric.name_for_ai_annotation(): {
                "reasoning": "reasoning for the value",
                "value": "Pass or Fail",
            }
            for metric in METRICS_CONFIG
        },
        indent=2,
    )

    return metric_definitions, response_format


def custom_eval_fn_metric(metric, predictions, targets):
    all_scores = [None] * len(predictions)
    all_justifications = [None] * len(predictions)
    judged_scores = []
    metric_values = asyncer.syncify(eval_metrics, raise_sync_error=False)(
        metric, predictions, targets
    )
    for i, judged_metric in enumerate(metric_values):
        if judged_metric is not None:
            judged_value = judged_metric.bool_value()
            all_scores[i] = judged_value
            all_justifications[i] = judged_metric.reasoning
            if judged_value is not None:
                judged_scores.append(judged_value)
    return mlflow.metrics.MetricValue(
        scores=all_scores,
        justifications=all_justifications,
        aggregate_results={
            "pass_percentage": 100
            * len(list(filter(lambda x: x, judged_scores)))
            / len(judged_scores)
            if len(judged_scores) > 0
            else 0,
            "count": len(judged_scores),
        },
    )


async def eval_metrics(
    metric, predictions, targets
) -> List[Optional[JudgedMetricValue]]:
    results: List[asyncer.SoonValue[JudgedMetricValue]] = []
    async with asyncer.create_task_group() as task_group:
        for prediction, target in zip(predictions, targets, strict=False):
            result = task_group.soonify(asyncer.asyncify(eval_metric_value_ai_judge))(
                metric, prediction, target
            )
            results.append(result)

    return [result.value for result in results]


def eval_metric_value_ai_judge(
    metric, prediction, target
) -> Optional[JudgedMetricValue]:
    human_guidelines = get_human_guidelines()
    human_guidelines_str: Optional[str] = None
    if target["urn"] in human_guidelines:
        human_guidelines_str = human_guidelines[target["urn"]].json()
    judged_metric = getattr(
        llm_judge_common_eval_fn(prediction, json.dumps(target), human_guidelines_str),
        metric,
    )

    return judged_metric


def overall_score_eval_fn(predictions, targets):
    all_scores = []
    all_justifications = []
    judged_scores = []
    human_guidelines = get_human_guidelines()
    for prediction, target in zip(predictions, targets, strict=False):
        table_human_guidelines_str: Optional[str] = None
        if target["urn"] in human_guidelines:
            table_human_guidelines_str = human_guidelines[target["urn"]].json()
        judged_metric = get_overall_score(
            llm_judge_common_eval_fn(
                prediction, json.dumps(target), table_human_guidelines_str
            )
        )
        if judged_metric is not None:
            all_scores.append(judged_metric.value)
            all_justifications.append(judged_metric.reasoning)
            # this is hack to keep only judged entries. reasoning in always present.
            if judged_metric.reasoning is not None:
                judged_scores.append(judged_metric.value)
        else:
            all_scores.append(0)
            all_justifications.append(None)

    return mlflow.metrics.MetricValue(
        scores=all_scores,
        justifications=all_justifications,
        aggregate_results={
            "mean": np.mean(judged_scores),
            "median": np.median(judged_scores),
            "count": len(judged_scores),
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
    mlflow.metrics.make_genai_metric_from_prompt that can only generate single metric from single prompt.
    """
    return mlflow.metrics.make_metric(
        eval_fn=functools.partial(custom_eval_fn_metric, metric_name),
        greater_is_better=True,
        name=metric_name,
    )


metric_overall_score = make_overall_score_metric()

ai_metrics = [
    *[make_custom_metric(metric.name) for metric in METRICS_CONFIG],
    metric_overall_score,
]


def model_fn(x):
    return x


def run_ai_annotations_experiment(run_name: str, run_description: Optional[str] = None):
    run = get_run_or_fail(run_name)

    artifact_path = "eval_results_table.json"
    table_descriptions = mlflow.load_table(
        artifact_file=artifact_path, run_ids=[run.info.run_id]
    )

    ai_annotation_run_name = get_ai_annotation_run_name(run_name)

    with mlflow.start_run(
        experiment_id=run.info.experiment_id,
        run_name=ai_annotation_run_name,
        description=run_description,
    ):
        mlflow.set_tag("evaluation_type", "ai_judge")
        mlflow.log_params({"ai_judge_model": AI_JUDGE_MODEL})

        mlflow.evaluate(
            model=model_fn,
            data=table_descriptions,
            predictions="description",
            evaluators="default",
            targets="entity_info",
            extra_metrics=[*ai_metrics],
        )
        # log current file as artifact
        mlflow.log_artifact("./run_ai_annotations.py")
        mlflow.log_artifact("./eval_config/eval_set_guidelines.yaml")


if __name__ == "__main__":
    typer.run(run_ai_annotations_experiment)
