import ast
import math
import os
import pathlib
import re
from typing import Dict, List, Tuple

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger
from pydantic import BaseModel, Field, ValidationError, parse_obj_as

from datahub_integrations.gen_ai.bedrock import BedrockModel
from datahub_integrations.gen_ai.description_v2 import (
    call_bedrock_llm,
    extract_metadata_for_urn,
    transform_table_info_for_llm,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import GlossaryInfo

TERM_SUGGESTION_GENERATION_MODEL: BedrockModel = parse_obj_as(
    BedrockModel,
    os.getenv(
        "TERM_SUGGESTION_GENERATION_BEDROCK_MODEL", BedrockModel.CLAUDE_3_HAIKU.value
    ),
)

# Suggestions with a confidence score strictly below this should be filtered out.
TERM_SUGGESTION_CONFIDENCE_THRESHOLD: float = 8.0
COLUMN_SPLIT_LENGTH = 30

_USE_REFLECTION = False
_REFLECTION_PROMPT_PATH = pathlib.Path(__file__).parent / "reflection_prompt.txt"
_PROMPT_PATH = pathlib.Path(__file__).parent / "term_suggestion_prompt.txt"
_TABLE_INFO_FOR_PROMPT = ["name", "description"]
_COLUMN_INFO_FOR_PROMPT = [
    "column_name",
    "descriptions",
    "sample_values",
    "datatype",
]


class TermSuggestionBundle(BaseModel):
    urn: str
    name: str
    reasoning: str
    confidence_score: float
    is_fake: bool | None = Field(default=None)


def parse_terms_list_obj(terms_list: List[dict]) -> List[TermSuggestionBundle]:
    parsed_terms_list: List[TermSuggestionBundle] = []
    for term_dict in terms_list:
        try:
            parsed_terms_list.append(TermSuggestionBundle.parse_obj(term_dict))
        except ValidationError as e:
            logger.info(f"Validation error for element {term_dict}: {e}")
    return parsed_terms_list


def parse_llm_output(
    text: str,
) -> Tuple[
    List[TermSuggestionBundle] | None,
    Dict[str, List[TermSuggestionBundle]] | None,
]:
    match = re.search(r"\{[\s\S]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        dict_str_cleaned = dict_str.strip()
        dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)
            parsed_extracted_dict: Dict[str, List[TermSuggestionBundle]] = {
                key: parse_terms_list_obj(value)
                for key, value in extracted_dict.items()
            }
            table_terms = parsed_extracted_dict.pop("table", None)
            return table_terms, parsed_extracted_dict
        except (SyntaxError, ValueError) as e:
            logger.info(f"Error evaluating dictionary: {e}")  # . Text: {text}")
            return None, None
    else:
        logger.info("No dictionary found in the text.")
        return None, None


def parse_llm_reflection_output(
    text: str,
) -> Dict[str, List[TermSuggestionBundle]] | None:
    match = re.search(r"\{[\s\S]*\}", text, re.DOTALL)
    if match:
        dict_str = match.group(0)
        dict_str_cleaned = dict_str.strip()
        dict_str_cleaned = re.sub("(?<=[a-z])'(?=[a-z])", "\\'", dict_str_cleaned)
        try:
            extracted_dict: dict = ast.literal_eval(dict_str_cleaned)
            parsed_extracted_dict: Dict[str, List[TermSuggestionBundle]] = {
                key: parse_terms_list_obj(value)
                for key, value in extracted_dict.items()
            }
            # table_terms = parsed_extracted_dict.pop("table", None)
            return parsed_extracted_dict
        except (SyntaxError, ValueError) as e:
            logger.info(f"Error evaluating dictionary: {e}")  # . Text: {text}")
            return None
    else:
        logger.info("No dictionary found in the text.")
        return None


def label_fake_column_terms(
    column_terms: Dict[str, List[TermSuggestionBundle]] | None,
    all_terms: List[str],
) -> Dict[str, List[TermSuggestionBundle]] | None:
    if isinstance(column_terms, dict):
        for column, terms in column_terms.items():
            updated_terms = []
            for term in terms:
                if isinstance(term, TermSuggestionBundle):
                    if term.name in all_terms:
                        term.is_fake = False
                    else:
                        term.is_fake = True
                updated_terms.append(term)
            column_terms[column] = updated_terms
    return column_terms


def generate_prompt(
    table_info: dict,
    column_info: dict,
    glossary_info: GlossaryInfo,
    prompt_path: str | None,
) -> str:
    if prompt_path is not None:
        logger.info(f"Reading prompt from {prompt_path}")
        prompt_template = pathlib.Path(prompt_path).read_text()
    else:
        logger.info(f"Reading default prompt from {_PROMPT_PATH}")
        prompt_template = _PROMPT_PATH.read_text()
    prompt = prompt_template.format(
        table_info=table_info,
        column_info=column_info,
        glossary_terms_info=glossary_info.glossary,
    )
    # logger.debug(f"Generated prompt: {prompt}")
    return prompt


def split_columns_list(columns: list[str], limit: int) -> list[list[str]]:
    column_count = len(columns)
    if column_count <= limit:
        return [columns]
    else:
        num_parts = math.ceil(len(columns) / limit)  # Calculate the number of parts
        part_size = math.ceil(len(columns) / num_parts)  # Calculate ideal part size
        # num_parts = -(-column_count // limit)
    column_splits = [
        columns[(i * part_size) : ((i + 1) * part_size)] for i in range(num_parts)
    ]
    return column_splits


def filter_table_information(table_info: dict, column_info: dict) -> tuple[dict, dict]:
    for column in column_info.keys():
        column_info[column].update(
            {
                "datatype": column_info[column]
                .get("metadata", {})
                .get("nativeDataType", "")
            }
        )
    table_info_filtered = {
        k: v for k, v in table_info.items() if k in _TABLE_INFO_FOR_PROMPT
    }
    column_info_filtered = {}
    for column_name, column_info_dict in column_info.items():
        column_info_filtered[column_name] = {
            k: v for k, v in column_info_dict.items() if k in _COLUMN_INFO_FOR_PROMPT
        }
    return table_info_filtered, column_info_filtered


def generate_reflection_prompt(
    preassigned_glossary_terms: Dict[str, List[TermSuggestionBundle]],
    column_info: dict,
    table_info: dict,
    glossary_info: GlossaryInfo,
) -> str:
    prompt_template = _REFLECTION_PROMPT_PATH.read_text()
    prompt = prompt_template.format(
        preassigned_glossary_terms=preassigned_glossary_terms,
        table_info=table_info,
        column_info=column_info,
        glossary_info=glossary_info.glossary,
    )
    return prompt


def get_term_recommendations_for_column_splits(
    column_splits: List[List[str]],
    table_info: dict,
    column_info: dict,
    glossary_info: GlossaryInfo,
    prompt_path: str | None,
) -> tuple[
    List[TermSuggestionBundle] | None, Dict[str, List[TermSuggestionBundle]] | None, str
]:
    column_terms: Dict[str, List[TermSuggestionBundle]] | None = None
    table_terms: List[TermSuggestionBundle] | None = None
    raw_llm_response: str = ""
    logger.debug(f"Column split lengths: {[len(split) for split in column_splits]}")
    for column_split in column_splits:
        column_split_info = {
            key: value for key, value in column_info.items() if key in column_split
        }
        prompt = generate_prompt(
            table_info,
            column_split_info,
            glossary_info=glossary_info,
            prompt_path=prompt_path,
        )
        raw_llm_response_for_column_split = call_bedrock_llm(
            prompt, model=TERM_SUGGESTION_GENERATION_MODEL, max_tokens=5000
        )
        table_terms, column_split_terms = parse_llm_output(
            raw_llm_response_for_column_split
        )

        if column_split_terms and _USE_REFLECTION:
            reflection_prompt = generate_reflection_prompt(
                table_info=table_info,
                column_info=column_info,
                glossary_info=glossary_info,
                preassigned_glossary_terms=column_split_terms,
            )
            logger.debug("Running Reflection...")
            reassessed_column_split_terms_raw = call_bedrock_llm(
                prompt=reflection_prompt,
                model=TERM_SUGGESTION_GENERATION_MODEL,
                max_tokens=5000,
            )
            reassessed_column_split_terms = parse_llm_reflection_output(
                reassessed_column_split_terms_raw
            )
            column_split_terms = reassessed_column_split_terms

        if isinstance(column_split_terms, dict):
            if isinstance(column_terms, dict):
                column_terms.update(column_split_terms)
            else:
                column_terms = column_split_terms
        raw_llm_response = (
            f"{raw_llm_response} \n\n {raw_llm_response_for_column_split}"
        )
    return table_terms, column_terms, raw_llm_response


def get_term_recommendations(
    table_urn: str,
    graph_client: DataHubGraph,
    glossary_info: GlossaryInfo,
    prompt_path: str | None = None,
) -> tuple[
    List[TermSuggestionBundle] | None,
    Dict[str, List[TermSuggestionBundle]] | None,
    str,
]:
    entity = graph_client.get_entity_semityped(table_urn)
    extracted_entity_info = extract_metadata_for_urn(entity, table_urn, graph_client)
    table_info, column_info = transform_table_info_for_llm(extracted_entity_info)
    table_info_filtered, column_info_filtered = filter_table_information(
        table_info, column_info
    )
    column_splits = split_columns_list(
        list(column_info_filtered.keys()), limit=COLUMN_SPLIT_LENGTH
    )
    table_terms, column_terms, raw_llm_response = (
        get_term_recommendations_for_column_splits(
            column_splits=column_splits,
            table_info=table_info_filtered,
            column_info=column_info_filtered,
            glossary_info=glossary_info,
            prompt_path=prompt_path,
        )
    )
    all_glossary_terms = []
    for term in glossary_info.glossary.values():
        all_glossary_terms.append(term["term_name"])
    if isinstance(table_terms, list):
        for term in table_terms:
            if term.name in all_glossary_terms:
                term.is_fake = False
            else:
                term.is_fake = True
    column_terms = label_fake_column_terms(column_terms, all_glossary_terms)
    return table_terms, column_terms, raw_llm_response
