from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import ast
import csv
import dataclasses
import json
import pathlib
import re
from typing import TypedDict

import tqdm
from graph_helper import create_datahub_graph

from datahub_integrations.gen_ai.bedrock import BedrockModel, call_bedrock_llm
from datahub_integrations.gen_ai.description_context import (
    extract_metadata_for_urn,
    transform_table_info_for_llm,
)

assert AI_EXPERIMENTATION_INITIALIZED


@dataclasses.dataclass(frozen=True)
class EntityRef:
    instance: str
    urn: str


class GeneratedDescription(TypedDict):
    instance: str
    urn: str
    table_description: str
    column_description: dict[str, str]


def read_llm_response_csv(
    csv_path: str | pathlib.Path,
) -> dict[EntityRef, GeneratedDescription]:
    parsed_csv: dict[EntityRef, GeneratedDescription] = {}
    with open(csv_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # print(row)
            instance = row["instance"]
            urn = row["urn"]
            parsed_csv[EntityRef(instance, urn)] = {
                "instance": instance,
                "urn": urn,
                "table_description": row["table_description"],
                "column_description": json.loads(row["column_description"]),
            }

    return parsed_csv


class ComparativeTableInfo(TypedDict):
    table_info: dict
    table_description_1: str
    table_description_2: str


def get_table_info_with_descriptions(
    dict1: dict[EntityRef, GeneratedDescription],
    dict2: dict[EntityRef, GeneratedDescription],
) -> dict[EntityRef, ComparativeTableInfo]:
    table_info_with_descriptions: dict[EntityRef, ComparativeTableInfo] = {}

    refs = set(dict1.keys()) & set(dict2.keys())
    ref: EntityRef
    for ref in tqdm.tqdm(refs):
        graph_client = create_datahub_graph(ref.instance)
        entity = graph_client.get_entity_semityped(ref.urn)
        extracted_entity_info = extract_metadata_for_urn(entity, ref.urn, graph_client)
        table_info, _ = transform_table_info_for_llm(extracted_entity_info)
        table_info_with_descriptions[ref] = {
            "table_info": table_info,
            "table_description_1": dict1[ref]["table_description"],
            "table_description_2": dict2[ref]["table_description"],
        }
    return table_info_with_descriptions


def get_llm_response_comparison_score_single(
    comparison: ComparativeTableInfo,
) -> str:
    table_info = comparison["table_info"]
    table_description_1 = comparison["table_description_1"]
    table_description_2 = comparison["table_description_2"]
    prompt = f"""\
Table Information:
<table_info>
{table_info}
</table_info>

Generated Table Descriptions:
Description 1:
{table_description_1}

Description 2:
{table_description_2}
Task:
Please compare the two generated table descriptions based on the provided table information. Provide a quality score for each description on a scale of 1 to 10, with 10 being the highest quality.

The score should be based on the following criteria:
- Accuracy: How well does the description match the provided table information?
- Clarity: Is the description easy to understand and clear?
- Completeness: Does the description cover all relevant aspects of the table information?
- Relevance: Is the description relevant and focused on the important details?

For each description, provide a detailed reasoning for the score, addressing each of the criteria listed above.
The scores for each description should be relative to each other.

A few additional notes:
- The description should not say something like "access should be restricted and monitored", "appropriate access controls should be in place", or anything similar. Such phrases are not relevant and do not help completeness.
- Section headers and subheaders help with clarity. Mixing multiple topics in a single paragraph hurts clarity.
- We do not a full listing of column names and types. Mentioning a few column names is fine.

Provide your output in the following dictionary format:

{{
    "table_description_1_score": {{"reasoning": "reasoning for the score", "score_computation_logic": "score computation logic in single line", "score": description score}},
    "table_description_2_score": {{"reasoning": "reasoning for the score", "score_computation_logic": "score computation logic in single line", "score": description score}},
}}

- Ensure that the dictionary is properly formatted and parsable.
- Do not use utf-8 or invalid Unicode characters in the response text.
- Avoid mismatching of braces and quotes: for example: "this is a "sample" text." has wrong arrangement of quotes which makes it non parsable.
"""
    llm_response = call_bedrock_llm(
        prompt, model=BedrockModel.CLAUDE_3_HAIKU, max_tokens=3000
    )
    return llm_response


def get_llm_response_comparison_score(
    table_info_with_descriptions: dict[EntityRef, ComparativeTableInfo],
) -> dict[EntityRef, str]:
    comparison_scores_dict: dict[EntityRef, str] = {}
    for ref in tqdm.tqdm(table_info_with_descriptions.keys()):
        # if ref.instance == "longtailcompanions":
        #    logger.debug(f"Skipping {ref}")
        #    continue
        # print(ref)
        comparison_scores_dict[ref] = get_llm_response_comparison_score_single(
            table_info_with_descriptions[ref]
        )
    return comparison_scores_dict


def prepare_csv_output(llm_output, dict1, dict2):
    # out_csv_format = ["urn", "instance", "csv1_desc", "csv2_desc", "csv1_score", "csv2_score", "csv1_reasoning", "csv2_reasoning"]
    formatted_llm_output = []
    for ref in llm_output.keys():
        assert llm_output.get(ref) is not None and llm_output.get(ref) is not None, (
            "table not present in csvs"
        )
        table1_description = dict1[ref].get("table_description")
        table2_description = dict2[ref].get("table_description")
        table_description_1_score = llm_output[ref].get("table_description_1_score")
        table_description_2_score = llm_output[ref].get("table_description_2_score")
        if table_description_1_score is not None:
            table1_score = table_description_1_score.get("score")
            table1_reasoning = table_description_1_score.get("reasoning")
            table1_scoring_logic = table_description_1_score.get(
                "score_computation_logic"
            )
        else:
            table1_score = None
            table1_reasoning = None
            table1_scoring_logic = None

        if table_description_2_score is not None:
            table2_score = table_description_2_score.get("score")
            table2_reasoning = table_description_2_score.get("reasoning")
            table2_scoring_logic = table_description_2_score.get(
                "score_computation_logic"
            )
        else:
            table2_score = None
            table2_reasoning = None
            table2_scoring_logic = None

        formatted_llm_output.append(
            [
                ref.urn,
                ref.instance,
                table1_description,
                table2_description,
                table1_score,
                table2_score,
                table1_reasoning,
                table2_reasoning,
                table1_scoring_logic,
                table2_scoring_logic,
            ]
        )
    return formatted_llm_output


def write_llm_output_to_csv(llm_responses, csv_path) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(
            [
                "urn",
                "instance",
                "csv1_desc",
                "csv2_desc",
                "csv1_score",
                "csv2_score",
                "csv1_reasoning",
                "csv2_reasoning",
                "csv1_score_computation_logic",
                "csv2_score_computation_logic",
            ]
        )
        for row in llm_responses:
            csvwriter.writerow(list(row))
    print("csv file created successfully!!!")


def parse_llm_output(text: str):
    match = re.search(r"\{[\s\S]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        # dict_str_cleaned = dict_str.replace("\n", " ").strip()
        # dict_str_cleaned = dict_str.strip()
        # dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        dict_str_cleaned = re.sub(r"\s+", " ", dict_str).strip()
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)
            assert (
                "table_description_1_score" in extracted_dict.keys()
                and "table_description_2_score" in extracted_dict.keys()
            ), "Could not find the description scores in the llm output"
            assert (
                extracted_dict["table_description_1_score"].get("score") is not None
                and extracted_dict["table_description_1_score"].get("score") is not None
                and extracted_dict["table_description_1_score"].get(
                    "score_computation_logic"
                )
                is not None
            ), (
                "LLM response for table_description_1 does not match the dessired output format"
            )
            assert (
                extracted_dict["table_description_2_score"].get("score") is not None
                and extracted_dict["table_description_2_score"].get("score") is not None
                and extracted_dict["table_description_2_score"].get(
                    "score_computation_logic"
                )
                is not None
            ), (
                "LLM response for table_description_2 does not match the dessired output format"
            )
            return extracted_dict

        except (SyntaxError, ValueError) as e:
            print(f"Error evaluating dictionary: {e}. Text: {text}")
            return {}
    else:
        print("No dictionary found in the text.")
        return {}
