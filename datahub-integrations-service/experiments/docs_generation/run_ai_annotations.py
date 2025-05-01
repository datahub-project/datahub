import mlflow
import mlflow.metrics
import mlflow.bedrock

mlflow.bedrock.autolog()

import datetime
from typing import Optional, List
from pydantic import BaseModel
import functools
import re
import json
import pandas as pd
import numpy as np
import os
import dotenv
import tenacity

dotenv.load_dotenv()

import numpy as np
import typer


from datahub_integrations.gen_ai.bedrock import call_bedrock_llm
from datahub_integrations.gen_ai.description_v2 import (
    transform_table_info_for_llm,
    ExtractedTableInfo,
    DESCRIPTION_GENERATION_MODEL,
)

# TODO: move to common
METRIC_NAMES = [
    "has_source_details",
    "has_downstream_usecases",
    "has_compliance_insights",
    "has_usage_tips",
    "is_confident",
]

EXPERIMENT_NAME = "docs_generation"
ANNOTATION_EXAMPLE_RUN_NAME = "human_annotations_initial_run"
mlflow.set_experiment(EXPERIMENT_NAME)


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


# TODO: move to common, combine with HumanJudgeVerdict
class AIJudgeVerdict(BaseModel):
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


# TODO: move to common
def get_run_or_fail(run_name):
    return mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"attributes.run_name='{run_name}'",
        output_format="list",
        order_by=["start_time DESC"],
    )[0]


class FewShotExample(BaseModel):
    entity_info: ExtractedTableInfo
    description: str
    ai_judge_verdict: AIJudgeVerdict

    def pretty_example(self) -> str:
        table_info, column_infos = transform_table_info_for_llm(self.entity_info)
        return f"""\
Provided Information:
Table Info:
{json.dumps(table_info.dict(exclude_none=True), indent=4)}
Column Infos:
{json.dumps({k: v.dict(exclude_none=True) for k, v in column_infos.items()}, indent=4)}

Generated Description To Evaluate:
{self.description}

Output:
{json.dumps(self.ai_judge_verdict.dict(exclude_none=True), indent=4)}
"""


@functools.cache
def build_few_shot_examples(run_name: str, limit: int = 3) -> List[FewShotExample]:
    # TODO: Handpick examples OR add examples that differ from AI eval results
    human_annotation_run = get_run_or_fail(run_name)

    human_eval_df = get_human_evals(human_annotation_run.info.run_id)

    examples = []
    for _, row in human_eval_df.iterrows():
        few_shot_example = FewShotExample(
            entity_info=ExtractedTableInfo.parse_obj(row["entity_info"]),
            description=row["description"],
            ai_judge_verdict=AIJudgeVerdict.parse_obj(
                {
                    metric_name: {
                        "value": "pass" if row[f"{metric_name}/score"] == 1 else "fail",
                        "reasoning": row[f"{metric_name}/justification"],
                    }
                    for metric_name in METRIC_NAMES
                }
            ),
        )
        examples.append(few_shot_example)
        if len(examples) >= limit:
            break

    return examples


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


LLM_JUDGE_PROMPT = """
You are a judge who is tasked with evaluating the quality of auto-generated table description. The description should be based on facts in provided information. You will be given generated table description and facts available about the table. You need to evaluate the quality of the description with respect the pre-defined metrics. Note that you need to provide a single like reasoning for your evaluation and  "Pass" or "Fail" result for each metric.

Metrics:
<has_source_details> 
    Whether description contains correct details about upstream source of table and/or transformations applied on source data to create this table, if available in provided information. Fail the metric if preceding criteria is not fulfilled or if description contains source details that are entirely absent in provided information, particularly upstream lineage.
</has_source_details>
<has_downstream_usecases> 
    Whether description contains correct details about downstream usecases of this table, if available in provided information. The description should always supplement each individual usecase mention with actual downstream from provided information. Fail the metric if preceding criteria is not fulfilled or if downstream usecases in description are entirely absent in provided information, particularly downstream lineage.
</has_downstream_usecases>
<has_compliance_insights> 
    Whether description contains correct details about presence or absence of PII(personally identifiable information) in table contents. Presence of person names, emails, addresses, phone numbers, etc must be hilighted as PII and not just mentioned as sensitive columns. Fail the metric if one of above columns is present yet description suggests table contains no PII columns or only hilights them as sensitive columns.
</has_compliance_insights>
<has_usage_tips> 
    Whether description contains correct and concise details about table content and way of its usage ? e.g. does it include grain of table, details about what data does table contain or which table is typically used with this table or which column is usually used for filtering, if available in provided information. Fail the metric if content and usage tips are incorrect or not present in provided information. 
</has_usage_tips>
<is_confident> 
    Whether the tone of description is confident and avoids weak / speculative phrases ? Fail this metric if description includes phrases like 'suggests', 'could be', 'likely', or 'is considered'.
</is_confident>

Provide your output in the JSON format:

{{
    "has_source_details": {{"reasoning": "reasoning for the value", "value": "Pass" or "Fail"}},
    "has_downstream_usecases": {{"reasoning": "reasoning for the value", "value": "Pass" or "Fail"}},
    "has_compliance_insights": {{"reasoning": "reasoning for the value", "value": "Pass" or "Fail"}},
    "has_usage_tips": {{"reasoning": "reasoning for the value", "value": "Pass" or "Fail"}},
    "is_confident": {{"reasoning": "reasoning for the value", "value": "Pass" or "Fail"}},
}}

NOTE:
- Include all metric evaluations in the output. The metric values should be strictly either "Pass" or "Fail".
- For Pass value, justification should include critical aspects that can be improved in description with reference to provided metric definition and provided information.
- For Fail value, justification should include critical aspects that led to failure of the metric.
- Make sure to not include any special characters (like quotes) in the reasoning string.

{examples}
Now your turn

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
# 1. Description should not explain why the table is fact table or dimension table.
# 2. Do we exclude empty lineage arrays from provided information
# 3. Change schema field urns to column name, table name
# 4. Looker explores and dashboards are mentioned as tables due to absence of the downstream subtypes.
# 5. Urn Links are probably not behaving correctly on UI


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
            except (SyntaxError, ValueError) as e1:
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
    fixed_text = call_bedrock_llm(
        prompt=fix_prompt, model=DESCRIPTION_GENERATION_MODEL, max_tokens=5000
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
def llm_judge_common_eval_fn(table_description, entity_info_str) -> AIJudgeVerdict:
    metrics = AIJudgeVerdict()

    if table_description is None:
        return metrics

    try:
        entity_info = ExtractedTableInfo.parse_raw(entity_info_str)
        mlflow.update_current_trace(
            tags={"tag": "ai_judge", "table_name": entity_info.table_name}
        )

        table_info, column_infos = transform_table_info_for_llm(entity_info)
        try:
            examples = build_few_shot_examples(ANNOTATION_EXAMPLE_RUN_NAME)
            examples_str = "Examples:\n" + "\n".join(
                [example.pretty_example() for example in examples]
            )
        except Exception as e:
            print(f"Error building few shot examples: {e}")
            examples_str = ""

        judge_prompt = LLM_JUDGE_PROMPT.format(
            examples=examples_str,
            table_description=table_description,
            table_info=json.dumps(table_info.dict(exclude_none=True), indent=4),
            column_infos=json.dumps(
                {k: v.dict(exclude_none=True) for k, v in column_infos.items()},
                indent=4,
            ),
        )

        judge_verdict = call_bedrock_llm(
            prompt=judge_prompt, model=DESCRIPTION_GENERATION_MODEL, max_tokens=5000
        )
        metrics = parse_llm_judge_output(judge_verdict)
    except Exception as e:
        if isinstance(e, tenacity.RetryError):
            e = e.last_attempt.exception()
        print(f"Error evaluating metric: {e}")
        return metrics

    return metrics


def custom_eval_fn_metric(metric, predictions, targets):
    scores = []
    justifications = []
    for prediction, target in zip(predictions, targets):
        judged_metric: Optional[MetricValue] = getattr(
            llm_judge_common_eval_fn(prediction, json.dumps(target)), metric
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
        judged_metric = llm_judge_common_eval_fn(
            prediction, json.dumps(target)
        ).overall_score
        if judged_metric is not None:
            scores.append(judged_metric.value)
            justifications.append(judged_metric.reasoning)
        else:
            scores.append(0)
            justifications.append(None)
    print(scores)

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
    mlflow.metrics.make_genai_metric_from_prompt that can only generate single metric from single prompt.
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

ai_metrics = [
    metric_has_source_details,
    metric_has_downstream_usecases,
    metric_has_compliance_insights,
    metric_has_usage_tips,
    metric_is_confident,
    metric_overall_score,
]


# TODO: move to common
def get_ai_annotation_run_name(run_name):
    original_run_name = run_name.replace("human_annotations_", "").replace(
        "ai_annotations_", ""
    )
    return f"ai_annotations_{original_run_name}"


def run_ai_annotations_experiment(run_name: str):
    run = get_run_or_fail(run_name)

    artifact_path = "eval_results_table.json"
    table_descriptions = mlflow.load_table(
        artifact_file=artifact_path, run_ids=[run.info.run_id]
    )

    ai_annotation_run_name = get_ai_annotation_run_name(run_name)

    with mlflow.start_run(
        experiment_id=run.info.experiment_id,
        run_name=ai_annotation_run_name,
    ):
        mlflow.set_tag("evaluation_type", "ai_judge")

        mlflow.evaluate(
            data=table_descriptions,
            predictions="description",
            evaluators="default",
            targets="entity_info",
            extra_metrics=[*ai_metrics],
        )
        # log current file as artifact
        mlflow.log_artifact("./run_ai_annotations.py")


if __name__ == "__main__":
    typer.run(run_ai_annotations_experiment)
