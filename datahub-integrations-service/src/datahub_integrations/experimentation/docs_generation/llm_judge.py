import functools
import glob
import json
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import mlflow
import tenacity
from diskcache import Cache
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.experimentation.docs_generation.eval_common import (
    METRICS_CONFIG,
    AIJudgeVerdict,
    HumanGuidelines,
)
from datahub_integrations.gen_ai.bedrock import call_bedrock_llm
from datahub_integrations.gen_ai.description_context import (
    ColumnMetadataInfo,
    ExtractedTableInfo,
    TableInfo,
    transform_table_info_for_llm,
)
from datahub_integrations.gen_ai.description_v3 import LARGE_TABLE_THRESHOLD
from datahub_integrations.gen_ai.model_config import BedrockModel

AI_JUDGE_MODEL = BedrockModel.CLAUDE_4_SONNET


JUDGE_CACHE_ENABLED = True
if JUDGE_CACHE_ENABLED and not TYPE_CHECKING:
    _cache = Cache("judge_cache")

    call_bedrock_llm = _cache.memoize()(call_bedrock_llm)


class FewShotExample(BaseModel):
    table_info: TableInfo
    column_infos: Dict[str, ColumnMetadataInfo]
    description: str
    ai_judge_verdict: "AIJudgeVerdict"

    def pretty_example(self) -> str:
        return f"""\
Provided Information:
Table Info:
{self.table_info.model_dump_json(exclude_none=True)}
Column Infos:
{json.dumps({k: v.model_dump(exclude_none=True) for k, v in self.column_infos.items()})}

Generated Description To Evaluate:
{self.description}

Output:
{self.ai_judge_verdict.model_dump_json(exclude_none=True, by_alias=True)}
"""


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
# - we probably need to give it some instructions on how interpret dbt siblings stuff
# - consistency in the output format - use headings or not, multiple vs single paragraph, etc


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
        entity_info = ExtractedTableInfo.model_validate_json(entity_info_str)
        mlflow.update_current_trace(tags={"tag": "ai_judge", "urn": entity_info.urn})

        table_info, column_infos = transform_table_info_for_llm(entity_info)
        try:
            examples = build_few_shot_examples()
            examples_str = "Examples:\n" + "\n".join(
                [example.pretty_example() for example in examples]
            )
        except Exception as e:
            logger.warning(f"Error building few shot examples: {e}")
            examples_str = ""

        table_metric_guidelines: Optional[HumanGuidelines] = None
        if table_metric_guidelines_str is not None:
            table_metric_guidelines = HumanGuidelines.model_validate_json(
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
            table_info=(table_info.model_dump_json(exclude_none=True)),
            column_infos=json.dumps(
                {
                    k: v.model_dump(
                        exclude_none=True,
                        include={"column_name", "description"}
                        if len(column_infos) > LARGE_TABLE_THRESHOLD
                        else None,
                    )
                    for k, v in column_infos.items()
                },
            ),
            table_level_guidelines=table_level_guidelines,
        )

        judge_verdict = call_bedrock_llm(
            prompt=judge_prompt, model=AI_JUDGE_MODEL, max_tokens=2048
        )
        metrics = parse_llm_judge_output(judge_verdict)
    except Exception as e:
        error_to_log = e
        if isinstance(e, tenacity.RetryError):
            error_to_log = e.last_attempt.result()
        logger.error(f"Error evaluating metric: {error_to_log}")

    return metrics


def gen_dynamic_metric_contents() -> Tuple[str, str]:
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


def parse_llm_judge_output(text: str) -> AIJudgeVerdict:
    metrics = AIJudgeVerdict()
    match = re.search(r"\{[^}]*\}", text, re.DOTALL)
    if match:
        try:
            return AIJudgeVerdict.model_validate_json(text)

        except (SyntaxError, ValueError) as e:
            try:
                fixed_text = try_fix_text_for_error(text, e)
                return AIJudgeVerdict.model_validate_json(fixed_text)
            except (SyntaxError, ValueError):
                logger.warning(f"Dictionary can not be parsed. Text:{text}, Error: {e}")

    else:
        logger.warning("No dictionary found in the text.")
        return metrics


@tenacity.retry(stop=tenacity.stop_after_attempt(2))
def try_fix_text_for_error(text: str, e: Exception) -> str:
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
        prompt=fix_prompt, model=AI_JUDGE_MODEL, max_tokens=2048
    )
    if not fixed_text.startswith("{"):
        json_start_idx = fixed_text.index("{")
        fixed_text = fixed_text[json_start_idx:]
    json.loads(
        fixed_text
    )  # this will raise an error if the text is not a valid JSON object
    return fixed_text


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
            example = FewShotExample.model_validate(example_data)
            examples.append(example)

        except Exception as e:
            logger.warning(f"Error loading example from {file_path}: {e}")
            continue

    return examples
